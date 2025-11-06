#!/usr/bin/env python3

"""
The MIT License(MIT)
Copyright(c) 2025 Tiago Santos
Copyright(c) 2016 Joseph Milazzo

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE 
WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, 
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Local-only library scanner and video normalizer with SQLite tracking.
- Scans watched folders for video files.
- Tracks processed files in SQLite.
- Always re-encode every candidate not marked 'ok' in DB.
- Video re-encode: AV1 (Intel QSV via VAAPI interop). Copies audio and subtitles.
- Logs to system log directory with rotation. Falls back to work_dir if needed.

DB schema (auto-created):
  files(path PRIMARY KEY, size INTEGER, mtime REAL, sha256 TEXT,
        last_checked REAL, status TEXT, note TEXT)

Status values: 'ok' (converted), 'error' (failed last try)

Requirements: Python 3, ffmpeg with VAAPI+QSV, sqlite3.pp

Usage:

Edit settings below
Run with "python3 media_convert_2.py"

# This is based off of the media-convert script created by Joseph Milazzo
# https://bitbucket.org/majora2007/media-convert/src/master/
"""

from __future__ import annotations
import os
import sys
import time
import logging
import logging.handlers as lh
import sqlite3
import subprocess
import signal
import shutil
from typing import Optional, Tuple

#######################################################################
#                            Configuration                             #
#######################################################################

# Output container and temp path
EXT = "mkv"  # container stays MKV
work_dir = "/home/media/"
DB_PATH = os.path.join(work_dir, "media_convert.db")
temp_file = os.path.join(work_dir, f"temp.{EXT}")

# Logging configuration
LOG_DIR = "/var/log/media-convert"  # fallback to work_dir if not writable
LOG_FILE = "media-convert.log"
LOG_LEVEL = logging.WARNING
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB per file
LOG_BACKUP_COUNT = 5

# Watched folders and convertible extensions
watched_folders = ["/var/lib/media/content/movies", "/var/lib/media/content/series"]
exclude = []
convertible_extensions = ["rmvb", "mkv", "avi", "mov", "wmv", "m4v", "mp4"]

# Transcode policy
TARGET_WIDTH = 3840
TARGET_HEIGHT = 2160

# Encoder controls (internal options only)
PRESET = "slow"       # maps to av1_qsv -preset
GQ = 22               # maps to av1_qsv -global_quality

# Behavior flags
DELETE = True         # delete source after successful encode
JUST_CHECK = False    # if True, only log the ffmpeg commands

#######################################################################
#                         Utilities / Logging                          #
#######################################################################

def setup_logger() -> logging.Logger:
    # Attempt system log dir first
    log_path = None
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        log_path = os.path.join(LOG_DIR, LOG_FILE)
        handler = lh.RotatingFileHandler(log_path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)
    except Exception:
        # Fallback to work_dir
        fallback_dir = os.path.join(work_dir, "logs")
        os.makedirs(fallback_dir, exist_ok=True)
        log_path = os.path.join(fallback_dir, LOG_FILE)
        handler = lh.RotatingFileHandler(log_path, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT)

    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler.setFormatter(fmt)

    logger = logging.getLogger(__name__)
    logger.setLevel(LOG_LEVEL)
    # Avoid duplicate handlers on re-run
    logger.handlers.clear()
    logger.addHandler(handler)

    # Also log to stderr for interactive runs at INFO+
    sh = logging.StreamHandler()
    sh.setLevel(LOG_LEVEL)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    logger.debug(f"Logger initialized at {log_path}")
    return logger


def normalize_path(path: str) -> str:
    return path.replace('\\', '/')


def to_target_naming(filename: str) -> str:
    parts = filename.split('.')
    parts[-1] = EXT
    return '.'.join(parts)


def file_signature(path: str) -> Tuple[int, float]:
    st = os.stat(path)
    return st.st_size, st.st_mtime

def run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)

def _run(cmd: str, cwd: str) -> tuple[int, str]:
    p = subprocess.run(cmd, shell=True, cwd=cwd, text=True,
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return p.returncode, (p.stderr or "") + (p.stdout or "")


#######################################################################
#                               Database                                #
#######################################################################

def db_connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            size INTEGER,
            mtime REAL,
            sha256 TEXT,
            last_checked REAL,
            status TEXT,
            note TEXT
        )
        """
    )
    conn.commit()
    return conn


def db_lookup(conn: sqlite3.Connection, path: str, size: int, mtime: float) -> Optional[Tuple]:
    cur = conn.execute(
        "SELECT path, size, mtime, sha256, last_checked, status, note FROM files WHERE path=?",
        (path,),
    )
    row = cur.fetchone()
    return row if row and row[1] == size and row[2] == mtime and row[5] == 'ok' else None


def db_upsert(conn: sqlite3.Connection, path: str, size: int, mtime: float, status: str, note: str = None) -> None:
    now = time.time()
    conn.execute(
        """
        INSERT INTO files(path, size, mtime, sha256, last_checked, status, note)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(path) DO UPDATE SET
            size=excluded.size,
            mtime=excluded.mtime,
            last_checked=excluded.last_checked,
            status=excluded.status,
            note=excluded.note
        """,
        (path, size, mtime, None, now, status, note),
    )
    conn.commit()

#######################################################################
#                        FFmpeg command builder                        #
#######################################################################

def probe_field(inp: str, entry: str) -> str:
    cmd = [
        "ffprobe", "-v", "error", "-probesize", "1M", 
        "-analyzeduration", "0", "-select_streams", "v:0",
        "-show_entries", f"stream={entry}",
        "-of", "default=nokey=1:noprint_wrappers=1", inp
    ]
    p = subprocess.run(cmd, check=False, capture_output=True, text=True)
    return (p.stdout or "").strip().lower()

def is_hdr(inp: str) -> bool:
    ct = probe_field(inp, "color_transfer")      # e.g. smpte2084, arib-std-b67
    cp = probe_field(inp, "color_primaries")     # e.g. bt2020
    return (
        ("smpte2084" in ct) or
        ("arib-std-b67" in ct) or   # HLG
        ("bt2020" in cp)
    )

#######################################################################
#                        FFmpeg command builder                        #
#######################################################################

def build_two_pass_cmds(input_path: str, output_path: str, sw_tonemap: bool) -> Tuple[str, str]:
    # Shared filter chain with tonemap + scale + hwmap(qsv)

    scale = "scale_vaapi=w='ceil(min(3840,iw)/8)*8':h='ceil(min(2160,ih)/8)*8':force_original_aspect_ratio=decrease:format=p010"
    hwmap = "hwmap=derive_device=qsv,format=qsv"
    vf = f"{scale},{hwmap}"
    if is_hdr(input_path):
        if sw_tonemap:
            tm = (
                "hwdownload,format=p010le,zscale=transferin=smpte2084:primariesin=bt2020:matrixin=bt2020nc:"
                "transfer=linear:primaries=bt709:matrix=bt709,"
                "format=gbrp16le,tonemap=tonemap=hable:desat=0,format=p010le,hwupload,format=vaapi"
            )
        else:
            tm = "tonemap_vaapi=matrix=bt709:primaries=bt709:transfer=bt709:format=p010"
        vf = f"{tm},{scale},{hwmap}"
  
    pass1 = (
        "ffmpeg -hide_banner -loglevel error -y "
        "-probesize 200M -analyzeduration 200M -fflags +genpts+discardcorrupt+nobuffer -err_detect ignore_err "
        "-init_hw_device vaapi=va:/dev/dri/renderD128 -init_hw_device qsv=qsv@va -filter_hw_device va "
        "-hwaccel vaapi -hwaccel_device va -hwaccel_output_format vaapi "
        f"-i \"{input_path}\" "
        f"-vf \"{vf}\" "
        f"-c:v av1_qsv -preset {PRESET} -extbrc 1 -look_ahead_depth 100 -global_quality {GQ} -async_depth 4 -pass 1 "
        "-g 120 -force_key_frames \"expr:gte(t,n_forced*2)\" -cluster_time_limit 5000 -cluster_size_limit 5242880 "
        "-an -f null /dev/null"
    )

    pass2 = (
        "ffmpeg -hide_banner -loglevel error -y "
        "-init_hw_device vaapi=va:/dev/dri/renderD128 -init_hw_device qsv=qsv@va -filter_hw_device va "
        "-hwaccel vaapi -hwaccel_device va -hwaccel_output_format vaapi "
        f"-i \"{input_path}\" "
        "-map 0 -map_chapters 0 -map_metadata 0 -c:a copy -c:s copy "
        f"-vf \"{vf}\" "
        f"-c:v av1_qsv -preset {PRESET} -extbrc 1 -look_ahead_depth 100 -global_quality {GQ} -async_depth 4 -pass 2 "
        "-g 120 -force_key_frames \"expr:gte(t,n_forced*2)\" -cluster_time_limit 5000 -cluster_size_limit 5242880 "
        "-bsf:v av1_metadata=color_primaries=1:transfer_characteristics=1:matrix_coefficients=1:color_range=tv "
        f"\"{output_path}\""
    )
    return pass1, pass2

#######################################################################
#                               Main                                   #
#######################################################################

def signal_handler(signum, frame):
    pass
    sys.exit(0)

def main() -> int:
    logger = setup_logger()
    logger.info("==== Media Convert V4 start ====")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    conn = db_connect(DB_PATH)
    logger.debug(f"DB opened at {DB_PATH}")

    # Collect candidate files
    paths = []
    for base_path in watched_folders:
        base_path = normalize_path(base_path)
        logger.info(f"Scanning {base_path}")
        t0 = time.time()
        for root, dirs, files in os.walk(base_path, topdown=True):
            dirs[:] = [d for d in dirs if d not in exclude]
            for fname in files:
                full = normalize_path(os.path.join(root, fname))
                ext = fname.split('.')[-1].lower()
                if ext in convertible_extensions:
                    paths.append(full)
        logger.info('Scan time %.0fs for %s' % (round(time.time()-t0, 0), base_path))

    logger.info(f"Candidates found: {len(paths)}")
    if not paths:
        logger.info("No files to process. Exiting.")
        return 0

    processed = 0
    skipped = 0
    failed = 0

    for in_path in paths:
        try:
            in_path = normalize_path(in_path)
            size, mtime = file_signature(in_path)
            logger.debug(f"File sig size={size} mtime={mtime} path={in_path}")

            # DB short-circuit: only files not recorded as ok are encoded
            row = db_lookup(conn, in_path, size, mtime)
            if row:
                skipped += 1
                logger.info(f"Skip (DB ok): {in_path}")
                continue

            out_path = to_target_naming(in_path)

            if JUST_CHECK:
                p1, p2 = build_two_pass_cmds(in_path, out_path if DELETE else temp_file)
                logger.info(f"PASS1: {p1}")
                logger.info(f"PASS2: {p2}")
                db_upsert(conn, in_path, size, mtime, status='ok', note='checked-only')
                continue

            # Build and run two-pass encode
            p1, p2 = build_two_pass_cmds(in_path, temp_file, False)

            logger.warning(f"Encoding start: {in_path}")
            logger.debug(f"Pass1 cmd: {p1} (cwd={work_dir})")
            r1, log1 = _run(p1, work_dir)
            logger.debug(f"Pass1 exit: {r1}")
            if r1 != 0:
                if "No mastering display data" in log1:
                    logger.warning("Pass 1 failed with missing HDR mastering data. Retrying with software tonemap fallback.")
                    p1, p2 = build_two_pass_cmds(in_path, temp_file, True)
                    logger.warning(f"Encoding start: {in_path}")
                    logger.debug(f"Pass1 cmd: {p1} (cwd={work_dir})")
                    r1 = subprocess.call(p1, shell=True, cwd=work_dir)
                if r1 != 0:
                    logger.warning("Pass 1 failed")
                    failed += 1
                    db_upsert(conn, in_path, size, mtime, status='error', note=f'pass1 exit {r1}')
                    logger.error(f"Pass 1 failed for {in_path}")
                    continue

            logger.debug(f"Pass2 cmd: {p2} (cwd={work_dir})")
            r2 = subprocess.call(p2, shell=True, cwd=work_dir)
            logger.debug(f"Pass2 exit: {r2}")
            if r2 == 0:
                in_path_original = in_path + ".old"
                while os.path.isfile(in_path_original):
                    in_path_original = in_path_original + ".old"
                try:
                    shutil.move(in_path, in_path_original)
                except Exception as e:
                    logger.error(f"Move original file failed: {e}")
                    failed += 1
                    continue
                try:
                    shutil.move(temp_file, out_path)  # cross-device safe
                    #os.replace(temp_file, out_path)
                except Exception as e:
                    logger.error(f"Move output failed: {e}")
                    failed += 1
                    continue
                if DELETE:
                    try:
                        os.remove(in_path_original)
                    except Exception as e:
                        logger.error(f"Delete source failed, will overwrite: {e}")
                new_size, new_mtime = file_signature(out_path)
                db_upsert(conn, out_path, new_size, new_mtime, status='ok', note='encoded-av1')
                processed += 1
                logger.info(f"Encoding success: {in_path} -> {out_path}")
            else:
                failed += 1
                db_upsert(conn, in_path, size, mtime, status='error', note=f'pass2 exit {r2}')
                logger.error(f"Pass 2 failed for {in_path}")
                if os.path.isfile(temp_file):
                    try:
                        os.remove(temp_file)
                        logger.debug("Temp file removed after failure")
                    except Exception as e:
                        logger.warning(f"Temp cleanup failed: {e}")
        except Exception as e:
            failed += 1
            logger.exception(f"Unhandled error on {in_path}: {e}")
            try:
                size, mtime = file_signature(in_path)
                db_upsert(conn, in_path, size, mtime, status='error', note=str(e))
            except Exception:
                logger.debug("DB upsert skipped due to signature error")

    logger.info(f"Summary: processed={processed} skipped={skipped} failed={failed}")
    logger.info("==== Media Convert V4 end ====")
    return 0


if __name__ == '__main__':
    sys.exit(main())
