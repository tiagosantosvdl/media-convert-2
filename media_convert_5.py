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
- Video re-encode: AV1 (Intel QSV via VAAPI interop). Copies selected audio, converts selected text subtitles, and skips other subtitle formats.
- Preserves HDR10/HLG, converts Dolby Vision profile 5 to HDR10, and uses
  HDR10-compatible base layers from supported Dolby Vision profiles.
- Logs to system log directory with rotation. Falls back to work_dir if needed.

DB schema (auto-created):
  files(path PRIMARY KEY, size INTEGER, mtime REAL, sha256 TEXT,
        last_checked REAL, status TEXT, note TEXT)

Status values: 'ok' (converted), 'error' (failed last try)

Requirements: Python 3, ffmpeg with VAAPI+QSV; libplacebo+Vulkan for Dolby Vision profile 5.

Usage:

Edit settings below
Run with "python3 media_convert_4.py"

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
import json
import fcntl
import shlex
from dataclasses import dataclass
from typing import Optional, Tuple

#######################################################################
#                            Configuration                             #
#######################################################################

# Output container and temp path
EXT = "mkv"  # container stays MKV
work_dir = "/home/media/"
DB_PATH = os.path.join(work_dir, "media_convert.db")
LOCK_PATH = os.path.join(work_dir, "media_convert.lock")

# Logging configuration
LOG_DIR = "/var/log/media-convert"  # fallback to work_dir if not writable
LOG_FILE = "media-convert.log"
LOG_LEVEL = logging.INFO
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
PRESET = "slower"       # maps to av1_qsv -preset
CQP = 18                # maps to av1_qsv fixed quantizer scale
MAX_GOP_FRAMES = 120    # bounds seek preroll while allowing earlier adaptive I-frames

# Behavior flags
DELETE = True         # delete source after successful encode
JUST_CHECK = False    # if True, only log the ffmpeg commands

# Stream language filters. Empty lists keep every tagged language.
# Untagged/undefined streams are always kept. Regional tags such as en-US also match en.
wanted_audio_languages: list[str] = [
    "en", "eng",                 # English
    "nl", "nld", "dut",        # Dutch
    "pt", "por", "pob",        # Portuguese / common Brazilian alias
    "pt-BR", "por-BR",          # Brazilian Portuguese regional forms
]
wanted_subtitle_languages: list[str] = [
    "en", "eng",                 # English
    "nl", "nld", "dut",        # Dutch
    "pt", "por", "pob",        # Portuguese / common Brazilian alias
    "pt-BR", "por-BR",          # Brazilian Portuguese regional forms
]

# List of supported subtitles
TEXT_SUBTITLE_CODECS = {
    "subrip",
    "srt",
    "ass",
    "ssa",
    "mov_text",
    "webvtt",
    "text",
    "ttml",
}

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


def temporary_path(candidate_index: int) -> str:
    return os.path.join(work_dir, f"temp.{os.getpid()}.{candidate_index}.{EXT}")


def command_text(cmd: list[str]) -> str:
    return shlex.join(cmd)


def unique_sidecar_path(path: str, suffix: str) -> str:
    candidate = path + suffix
    while os.path.lexists(candidate):
        candidate += suffix
    return candidate


def ensure_output_path_available(input_path: str, output_path: str) -> None:
    if os.path.abspath(input_path) != os.path.abspath(output_path) and os.path.lexists(output_path):
        raise FileExistsError(f"Refusing to overwrite existing output: {output_path}")


def install_output(input_path: str, temp_path: str, output_path: str) -> str:
    ensure_output_path_available(input_path, output_path)
    backup_path = unique_sidecar_path(input_path, ".old")
    shutil.move(input_path, backup_path)

    try:
        shutil.move(temp_path, output_path)
    except Exception as move_error:
        failed_output_path = None
        if os.path.lexists(output_path):
            failed_output_path = unique_sidecar_path(output_path, ".failed")
            try:
                shutil.move(output_path, failed_output_path)
            except Exception as preserve_error:
                raise RuntimeError(
                    f"Output installation failed and the partial output could not be moved; "
                    f"original remains at {backup_path}: {preserve_error}"
                ) from move_error

        try:
            shutil.move(backup_path, input_path)
        except Exception as restore_error:
            partial_note = (
                f"; partial output retained at {failed_output_path}"
                if failed_output_path else ""
            )
            raise RuntimeError(
                f"Output installation failed and original restoration failed; "
                f"original remains at {backup_path}{partial_note}: {restore_error}"
            ) from move_error

        partial_note = (
            f"; partial output retained at {failed_output_path}"
            if failed_output_path else ""
        )
        raise RuntimeError(
            f"Output installation failed; original restored{partial_note}: {move_error}"
        ) from move_error

    return backup_path


_active_process: Optional[subprocess.Popen] = None


def run(cmd: list[str], cwd: str, logger: logging.Logger) -> tuple[int, str]:
    global _active_process

    try:
        process = subprocess.Popen(
            cmd,
            cwd=cwd,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
    except OSError as e:
        return 127, str(e)

    _active_process = process
    try:
        stdout, stderr = process.communicate()
    finally:
        _active_process = None

    for line in (stdout or "").splitlines():
        logger.debug(line)
    for line in (stderr or "").splitlines():
        if process.returncode:
            logger.error(line)
        else:
            logger.debug(line)

    return process.returncode, (stderr or "") + (stdout or "")


def acquire_process_lock(lock_path: str):
    lock_handle = open(lock_path, "a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except Exception:
        lock_handle.close()
        raise
    return lock_handle


def cleanup_temp(path: str, logger: logging.Logger) -> None:
    if not os.path.lexists(path):
        return
    try:
        os.remove(path)
        logger.debug(f"Temporary file removed: {path}")
    except Exception as e:
        logger.warning(f"Temporary file cleanup failed for {path}: {e}")


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
#                         HDR / Dolby Vision probe                    #
#######################################################################

HDR_SDR = "sdr"
HDR10_PQ = "hdr10-pq"
HDR_HLG = "hlg"
DOVI_PROFILE_5 = "dovi-profile-5"


class UnsupportedVideoError(RuntimeError):
    pass


@dataclass(frozen=True)
class VideoInfo:
    color_transfer: str
    color_primaries: str
    color_space: str
    color_range: str
    codec_name: str = ""
    dovi_profile: Optional[int] = None
    dovi_compatibility_id: Optional[int] = None
    dovi_el_present: bool = False
    dovi_bl_present: bool = False
    hdr10_plus: bool = False
    has_mastering_display: bool = False
    has_content_light_level: bool = False


def optional_int(value) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def probe_video_info(inp: str) -> VideoInfo:
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-read_intervals", "%+#1",
        "-show_streams",
        "-show_frames",
        "-of", "json",
        inp,
    ]
    p = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"ffprobe failed: {(p.stderr or '').strip()}")

    try:
        probe = json.loads(p.stdout or "{}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid ffprobe JSON: {e}") from e

    streams = probe.get("streams", [])
    if not streams:
        raise RuntimeError("No video stream found")

    stream = streams[0]
    stream_side_data = stream.get("side_data_list", [])
    frame_side_data = [
        side_data
        for frame in probe.get("frames", [])
        for side_data in frame.get("side_data_list", [])
    ]
    all_side_data = stream_side_data + frame_side_data
    side_data_names = [
        str(side_data.get("side_data_type", "")).lower()
        for side_data in all_side_data
    ]
    dovi = next(
        (
            side_data for side_data in stream_side_data
            if "dovi configuration record" in side_data.get("side_data_type", "").lower()
        ),
        None,
    )

    return VideoInfo(
        color_transfer=str(stream.get("color_transfer", "")).lower(),
        color_primaries=str(stream.get("color_primaries", "")).lower(),
        color_space=str(stream.get("color_space", "")).lower(),
        color_range=str(stream.get("color_range", "")).lower(),
        codec_name=str(stream.get("codec_name", "")).lower(),
        dovi_profile=optional_int(dovi.get("dv_profile")) if dovi else None,
        dovi_compatibility_id=(
            optional_int(dovi.get("dv_bl_signal_compatibility_id")) if dovi else None
        ),
        dovi_el_present=bool(optional_int(dovi.get("el_present_flag"))) if dovi else False,
        dovi_bl_present=bool(optional_int(dovi.get("bl_present_flag"))) if dovi else False,
        hdr10_plus=any(
            "hdr10+" in name or "smpte2094-40" in name
            for name in side_data_names
        ),
        has_mastering_display=any(
            "mastering display metadata" in name for name in side_data_names
        ),
        has_content_light_level=any(
            "content light level metadata" in name for name in side_data_names
        ),
    )


def hdr_mode(info: VideoInfo) -> str:
    if info.dovi_profile == 5:
        if not info.dovi_bl_present:
            raise UnsupportedVideoError("Dolby Vision profile 5 has no base layer")
        return DOVI_PROFILE_5

    if info.dovi_profile == 7:
        if not info.dovi_bl_present:
            raise UnsupportedVideoError("Dolby Vision profile 7 has no HDR10 base layer")
        return HDR10_PQ

    if info.dovi_profile == 8:
        if not info.dovi_bl_present:
            raise UnsupportedVideoError("Dolby Vision profile 8 has no base layer")
        if info.dovi_compatibility_id == 1:
            return HDR10_PQ
        raise UnsupportedVideoError(
            "Only HDR10-compatible Dolby Vision profile 8.1 is supported; "
            f"compatibility id is {info.dovi_compatibility_id!r}"
        )

    if info.dovi_profile == 10:
        if info.codec_name != "av1":
            raise UnsupportedVideoError(
                "Dolby Vision profile 10 requires an AV1 base layer; "
                f"codec is {info.codec_name or 'unknown'!r}"
            )
        if not info.dovi_bl_present:
            raise UnsupportedVideoError("Dolby Vision profile 10 has no base layer")
        if info.dovi_compatibility_id == 1:
            return HDR10_PQ
        raise UnsupportedVideoError(
            "Only HDR10-compatible Dolby Vision profile 10.1 is supported; "
            f"compatibility id is {info.dovi_compatibility_id!r}"
        )

    if info.dovi_profile is not None:
        raise UnsupportedVideoError(
            f"Dolby Vision profile {info.dovi_profile} is not supported safely"
        )

    if "smpte2084" in info.color_transfer:
        return HDR10_PQ
    if "arib-std-b67" in info.color_transfer:
        return HDR_HLG
    return HDR_SDR


def describe_video(info: VideoInfo, mode: str) -> str:
    details = [
        f"mode={mode}",
        f"codec={info.codec_name or 'unknown'}",
        f"transfer={info.color_transfer or 'unknown'}",
        f"primaries={info.color_primaries or 'unknown'}",
        f"matrix={info.color_space or 'unknown'}",
    ]
    details += [
        f"hdr10_plus={info.hdr10_plus}",
        f"mastering_display={info.has_mastering_display}",
        f"content_light_level={info.has_content_light_level}",
    ]
    if info.dovi_profile is not None:
        details += [
            f"dovi_profile={info.dovi_profile}",
            f"dovi_compatibility_id={info.dovi_compatibility_id}",
            f"dovi_el_present={info.dovi_el_present}",
        ]
    return " ".join(details)


def validate_output(path: str, mode: str) -> VideoInfo:
    if not os.path.isfile(path) or os.path.getsize(path) <= 0:
        raise RuntimeError(f"Encoded output is missing or empty: {path}")

    info = probe_video_info(path)
    if info.codec_name != "av1":
        raise RuntimeError(
            f"Encoded output video codec is {info.codec_name or 'unknown'}, expected AV1"
        )

    if mode in {HDR10_PQ, DOVI_PROFILE_5}:
        if (
            info.color_transfer != "smpte2084"
            or info.color_primaries != "bt2020"
            or info.color_space != "bt2020nc"
        ):
            raise RuntimeError(
                "Encoded HDR10 output has incorrect color signaling: "
                f"transfer={info.color_transfer!r}, "
                f"primaries={info.color_primaries!r}, matrix={info.color_space!r}"
            )
    elif mode == HDR_HLG:
        if (
            info.color_transfer != "arib-std-b67"
            or info.color_primaries != "bt2020"
            or info.color_space != "bt2020nc"
        ):
            raise RuntimeError(
                "Encoded HLG output has incorrect color signaling: "
                f"transfer={info.color_transfer!r}, "
                f"primaries={info.color_primaries!r}, matrix={info.color_space!r}"
            )
    return info

#######################################################################
#                Check and prepare subtitle conversion                #
#######################################################################

# Common ISO 639-1, terminology, and bibliographic aliases.
LANGUAGE_ALIAS_GROUPS = (
    {"ar", "ara"}, {"cs", "ces", "cze"}, {"da", "dan"},
    {"de", "deu", "ger"}, {"el", "ell", "gre"}, {"en", "eng"},
    {"es", "spa"}, {"fi", "fin"}, {"fr", "fra", "fre"},
    {"he", "heb"}, {"hi", "hin"}, {"hu", "hun"}, {"id", "ind"},
    {"it", "ita"}, {"ja", "jpn"}, {"ko", "kor"},
    {"nl", "nld", "dut"}, {"no", "nor"}, {"pl", "pol"},
    {"pt", "por", "pob"}, {"ro", "ron", "rum"}, {"ru", "rus"},
    {"sk", "slk", "slo"}, {"sv", "swe"}, {"th", "tha"},
    {"tr", "tur"}, {"uk", "ukr"}, {"vi", "vie"},
    {"zh", "zho", "chi"},
)
UNTAGGED_LANGUAGES = {"", "und", "unk"}


def language_keys(language: Optional[str]) -> set[str]:
    normalized = str(language or "").strip().lower().replace("_", "-")
    primary = normalized.split("-", 1)[0]
    keys = {normalized, primary}
    for aliases in LANGUAGE_ALIAS_GROUPS:
        if keys & aliases:
            keys.update(aliases)
            break
    return keys


def language_is_wanted(language: Optional[str], wanted: list[str]) -> bool:
    stream_keys = language_keys(language)
    if stream_keys & UNTAGGED_LANGUAGES:
        return True
    if not wanted:
        return True
    wanted_keys = set().union(*(language_keys(item) for item in wanted))
    return bool(stream_keys & wanted_keys)


def stream_mapping_args(path: str) -> list[str]:
    probe_cmd = [
        "ffprobe",
        "-v", "error",
        "-show_entries",
        "stream=index,codec_name,codec_type:stream_tags=language:"
        "stream_disposition=default,original",
        "-of", "json",
        path,
    ]
    probe = json.loads(subprocess.check_output(probe_cmd))
    logger = logging.getLogger(__name__)

    streams = probe.get("streams", [])
    audio_streams = [
        stream for stream in streams if stream.get("codec_type") == "audio"
    ]
    original_audio_indices = {
        stream["index"]
        for stream in audio_streams
        if (stream.get("disposition") or {}).get("original") == 1
    }
    wanted_audio_present = any(
        language_is_wanted(
            (stream.get("tags") or {}).get("language"),
            wanted_audio_languages,
        )
        for stream in audio_streams
    )
    if not original_audio_indices and audio_streams and not wanted_audio_present:
        original_audio_indices.add(audio_streams[0]["index"])

    args: list[str] = []
    out_sub_idx = 0

    for stream in streams:
        stream_type = stream.get("codec_type")
        if stream_type not in {"audio", "subtitle"}:
            continue

        index = stream["index"]
        codec = stream.get("codec_name")
        language = (stream.get("tags") or {}).get("language")
        wanted = (
            wanted_audio_languages
            if stream_type == "audio"
            else wanted_subtitle_languages
        )

        language_wanted = language_is_wanted(language, wanted)
        is_original_audio = (
            stream_type == "audio" and index in original_audio_indices
        )
        if not language_wanted and not is_original_audio:
            logger.info(
                f"Dropping unwanted {stream_type} stream={index} "
                f"language={language} codec={codec or 'unknown'} path={path}"
            )
            continue

        if stream_type == "audio":
            if not language_wanted:
                logger.info(
                    f"Keeping original audio outside language list stream={index} "
                    f"language={language} codec={codec or 'unknown'} path={path}"
                )
            args += ["-map", f"0:{index}"]
            continue

        if codec not in TEXT_SUBTITLE_CODECS:
            logger.warning(
                f"Skipping unsupported bitmap/non-text subtitle "
                f"stream={index} language={language or 'untagged'} "
                f"codec={codec or 'unknown'} path={path}"
            )
            continue

        args += ["-map", f"0:{index}"]
        args += [f"-c:s:{out_sub_idx}", "srt"]
        out_sub_idx += 1

    return args

#######################################################################
#                        FFmpeg command builder                        #
#######################################################################

def build_cmd(
    input_path: str,
    output_path: str,
    software_fallback: bool = False,
    video_info: Optional[VideoInfo] = None,
) -> list[str]:
    info = video_info or probe_video_info(input_path)
    mode = hdr_mode(info)
    scale = f"scale_vaapi=w='ceil(min({TARGET_WIDTH},iw)/8)*8':h='ceil(min({TARGET_HEIGHT},ih)/8)*8':force_original_aspect_ratio=decrease:format=p010"
    hwmap = "hwmap=derive_device=qsv,format=qsv"
    vf = f"{scale},{hwmap}"
    accel = [
        "-hwaccel", "vaapi",
        "-hwaccel_device", "va",
        "-hwaccel_output_format", "vaapi",
    ]
    hardware_init = [
        "-init_hw_device", "vaapi=va:/dev/dri/renderD128",
        "-init_hw_device", "qsv=qsv@va",
        "-filter_hw_device", "va",
    ]

    if mode == DOVI_PROFILE_5:
        hardware_init = [
            "-init_hw_device", "vaapi=va:/dev/dri/renderD128",
            "-init_hw_device", "qsv=qsv@va",
            "-init_hw_device", "vulkan=vk@va",
            "-filter_hw_device", "vk",
        ]
        dovi_render = (
            "libplacebo=apply_dolbyvision=true:color_primaries=bt2020:"
            "color_trc=smpte2084:colorspace=bt2020nc:range=tv:format=p010"
        )
        if software_fallback:
            accel = []
            software_scale = (
                f"scale=w='ceil(min({TARGET_WIDTH},iw)/8)*8':"
                f"h='ceil(min({TARGET_HEIGHT},ih)/8)*8':"
                "force_original_aspect_ratio=decrease"
            )
            vf = (
                f"format=p010le,hwupload,{dovi_render},"
                f"hwdownload,format=p010le,{software_scale}"
            )
        else:
            vf = (
                f"hwmap=derive_device=vulkan,format=vulkan,{dovi_render},"
                f"hwmap=derive_device=vaapi,format=vaapi,{scale},{hwmap}"
            )
    elif software_fallback:
        accel = []
        vf = f"format=p010le,hwupload,{scale},{hwmap}"

    if mode in {HDR10_PQ, DOVI_PROFILE_5}:
        color_args = [
            "-color_primaries", "bt2020",
            "-color_trc", "smpte2084",
            "-colorspace", "bt2020nc",
            "-color_range", "tv",
        ]
        av1_color_metadata = (
            "color_primaries=9:transfer_characteristics=16:"
            "matrix_coefficients=9:color_range=0"
        )
    elif mode == HDR_HLG:
        color_args = [
            "-color_primaries", "bt2020",
            "-color_trc", "arib-std-b67",
            "-colorspace", "bt2020nc",
            "-color_range", "tv",
        ]
        av1_color_metadata = (
            "color_primaries=9:transfer_characteristics=18:"
            "matrix_coefficients=9:color_range=0"
        )
    else:
        color_args = [
            "-color_primaries", "bt709",
            "-color_trc", "bt709",
            "-colorspace", "bt709",
            "-color_range", "tv",
        ]
        av1_color_metadata = (
            "color_primaries=1:transfer_characteristics=1:"
            "matrix_coefficients=1:color_range=0"
        )

    return [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-y",
        *hardware_init,
        *accel,
        "-i", input_path,
        "-map", "0:v:0",
        *stream_mapping_args(input_path),
        "-map_chapters", "0",
        "-map_metadata", "0",
        "-c:a", "copy",
        "-vf:v:0", vf,
        *color_args,
        "-c:v:0", "av1_qsv",
        "-preset", PRESET,
        "-q:v:0", str(CQP),
        "-async_depth", "4",
        "-g", str(MAX_GOP_FRAMES),
        "-adaptive_i", "1",
        "-bsf:v:0", f"av1_metadata={av1_color_metadata}",
        output_path,
    ]

#######################################################################
#                               Main                                   #
#######################################################################

def signal_handler(signum, frame):
    process = _active_process
    if process is not None and process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    sys.exit(128 + signum)


def main() -> int:
    logger = setup_logger()
    logger.info("==== Media Convert V4 start ====")
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    os.makedirs(work_dir, exist_ok=True)
    try:
        lock_handle = acquire_process_lock(LOCK_PATH)
    except BlockingIOError:
        logger.error(f"Another media-convert process holds {LOCK_PATH}")
        return 2

    conn = None
    try:
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
                    ext = fname.rsplit('.', 1)[-1].lower()
                    if ext in convertible_extensions:
                        paths.append(full)
            logger.info(
                "Scan time %.0fs for %s",
                round(time.time() - t0, 0),
                base_path,
            )

        logger.info(f"Candidates found: {len(paths)}")
        if not paths:
            logger.info("No files to process. Exiting.")
            return 0

        processed = 0
        checked = 0
        skipped = 0
        failed = 0

        for candidate_index, in_path in enumerate(paths):
            temp_path = temporary_path(candidate_index)
            try:
                in_path = normalize_path(in_path)
                size, mtime = file_signature(in_path)
                logger.debug(f"File sig size={size} mtime={mtime} path={in_path}")

                # DB short-circuit: only files not recorded as ok are encoded
                row = db_lookup(conn, in_path, size, mtime)
                if row:
                    skipped += 1
                    logger.debug(f"Skip (DB ok): {in_path}")
                    continue

                out_path = to_target_naming(in_path)
                ensure_output_path_available(in_path, out_path)
                if os.path.lexists(temp_path):
                    raise FileExistsError(f"Refusing to overwrite stale temp file: {temp_path}")

                video_info = probe_video_info(in_path)
                mode = hdr_mode(video_info)
                logger.info(f"Video format: {describe_video(video_info, mode)} path={in_path}")

                if (
                    mode == HDR10_PQ
                    and video_info.dovi_profile == 7
                    and video_info.dovi_el_present
                ):
                    logger.warning(
                        "Dolby Vision profile 7 enhancement layer will not be retained; "
                        f"encoding its HDR10 base layer: {in_path}"
                    )
                if video_info.hdr10_plus:
                    logger.info(
                        "HDR10+ input detected; output dynamic metadata will be verified "
                        f"after encoding: {in_path}"
                    )

                cmd = build_cmd(in_path, temp_path, False, video_info)
                if JUST_CHECK:
                    checked += 1
                    logger.info(f"Check command: {command_text(cmd)} (cwd={work_dir})")
                    continue

                logger.warning(f"Encoding start: {in_path}")
                logger.info(f"Encoding cmd: {command_text(cmd)} (cwd={work_dir})")
                return_code, command_log = run(cmd, work_dir, logger)
                logger.info(f"Encoding exit: {return_code}")

                if return_code != 0:
                    logger.warning(
                        "Encoding failed. Retrying with software decode/filter fallback."
                    )
                    cmd = build_cmd(in_path, temp_path, True, video_info)
                    logger.warning(f"Encoding start: {in_path}")
                    logger.info(f"Encoding cmd: {command_text(cmd)} (cwd={work_dir})")
                    return_code, command_log = run(cmd, work_dir, logger)
                    logger.info(f"Encoding exit: {return_code}")

                if return_code != 0:
                    detail = command_log.strip().splitlines()
                    last_error = detail[-1] if detail else "no ffmpeg error text"
                    raise RuntimeError(
                        f"ffmpeg exit {return_code}: {last_error}"
                    )

                output_info = validate_output(temp_path, mode)
                logger.info(
                    f"Validated output: "
                    f"{describe_video(output_info, hdr_mode(output_info))}"
                )

                if video_info.hdr10_plus and not output_info.hdr10_plus:
                    logger.warning(
                        "HDR10+ dynamic metadata was not retained; "
                        f"output is standard HDR10: {in_path}"
                    )

                missing_static_metadata = []
                if (
                    video_info.has_mastering_display
                    and not output_info.has_mastering_display
                ):
                    missing_static_metadata.append("mastering-display")
                if (
                    video_info.has_content_light_level
                    and not output_info.has_content_light_level
                ):
                    missing_static_metadata.append("content-light-level")
                if missing_static_metadata:
                    logger.warning(
                        "Static HDR metadata was not retained: "
                        f"{', '.join(missing_static_metadata)} path={in_path}"
                    )

                backup_path = install_output(in_path, temp_path, out_path)
                new_size, new_mtime = file_signature(out_path)
                db_upsert(
                    conn,
                    out_path,
                    new_size,
                    new_mtime,
                    status='ok',
                    note=f'encoded-av1-{mode}',
                )

                if DELETE:
                    try:
                        os.remove(backup_path)
                    except Exception as e:
                        logger.warning(
                            f"Could not delete original backup {backup_path}: {e}"
                        )
                else:
                    logger.info(f"Original retained at: {backup_path}")

                processed += 1
                logger.info(f"Encoding success: {in_path} -> {out_path}")

            except Exception as e:
                failed += 1
                logger.exception(f"Processing failed for {in_path}: {e}")
                cleanup_temp(temp_path, logger)
                try:
                    if os.path.isfile(in_path):
                        size, mtime = file_signature(in_path)
                        db_upsert(
                            conn,
                            in_path,
                            size,
                            mtime,
                            status='error',
                            note=str(e),
                        )
                except Exception:
                    logger.debug("DB upsert skipped due to signature error")

        logger.info(
            f"Summary: processed={processed} checked={checked} "
            f"skipped={skipped} failed={failed}"
        )
        logger.info("==== Media Convert V4 end ====")
        return 1 if failed else 0
    finally:
        if conn is not None:
            conn.close()
        lock_handle.close()

if __name__ == '__main__':
    sys.exit(main())
