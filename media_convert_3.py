# The MIT License(MIT)
# Copyright(c) 2019 Tiago Santos
# Copyright(c) 2016 Joseph Milazzo
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation
# files(the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy,
# modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE 
# WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
# COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, 
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# This script will convert all the video files in your library to one format
# The settings below are designed to optimize streaming to Chromecast
#
# Requirements:
# Python 3
# python-psutil
# python-mediainfo
# python-paramiko
# ffmpeg
#
# NEW VERSION:
# This script implements running the ffmpeg conversion remotely
# To do so, another computer will have to have a folder with the ffmpeg binaries and a running SSH Server
# The file to be converted is sent via SFTP, and then the command is executed via SSH
# The conversion will only be done remotely if video encoding is needed
#
# Usage:
#
# Edit settings below
# Run with "python3 media_convert_2.py"
# This script has been tested on Linux only, but should work on windows
# To run on Windows, be sure to remove the "nice -n 20" part from the beggining of ffmpeg_base_cmd
# and add the ffmpeg folder to PATH or insert the full path on ffmpeg_base_cmd

# This is based off of the media-convert script created by Joseph Milazzo
# https://bitbucket.org/majora2007/media-convert/src/master/

from collections import defaultdict
import os
import logging
from pymediainfo import MediaInfo
import paramiko
import subprocess
import signal
import psutil
import time
import sys

#######################################################################
#                            Variables                                #
#######################################################################

# Desired extension for files. Best container for streaming to chromecast is mp4
global EXT
EXT = 'mp4'

# Where to store temporary and log files
work_dir = '/home/plex/'

# Temporary enconding file
temp_file = work_dir + 'temp.' + EXT

# A list of directories to scan
watched_folders = ['/home/plex/Classes', '/home/plex/Movies', '/home/plex/Series']
exclude = []

# Conditions for video recoding
MAX_BITRATE = 7000000
MAX_HEIGHT = 1080
MAX_WIDTH = 1920
VIDEO_CODEC = "AVC"
VIDEO_PROFILE = "Main"

# Recode all videos ending with these extensions
valid_extensions = ['rmvb', 'mkv', 'avi', 'mov', 'wmv']

# Conditions for audio recoding. Chromecast may force sorround audio on stereo TVs, so force channels to 2 to convert all files to stereo audio
MAX_CHANNELS = 2
AUDIO_CODEC = "AAC"

# FFMPEG parameters
ffmpeg_base_cmd = "nice -n 20 ffmpeg -loglevel error -hide_banner -y -i "
ffmpeg_video_encode = " -c:v libx264 -preset faster -tune zerolatency -profile:v main -pix_fmt yuv420p -crf 22 -maxrate " + str(MAX_BITRATE) + " -bufsize " + str(int(MAX_BITRATE/2)) + " -vf \"scale=\'min(" + str(MAX_WIDTH) + ",iw)\':\'min(" + str(MAX_HEIGHT) + ",ih)\':force_original_aspect_ratio=decrease\""
ffmpeg_audio_encode = " -c:a aac -ac 2 -b:a 192k"
ffmpeg_middle_cmd = " -max_muxing_queue_size 1024 -map_metadata -1 -movflags +faststart"

# Flag to denote whether to delete source files after successfull encode
DELETE = True

# Flag to denote whether to just run MediaInfo on files
JUST_CHECK = False

# Verbosity level on log file
LOG_LEVEL = logging.INFO

#######################################################################
#                       Remote Conversion (SSH)                       #
#######################################################################

# If enabled, when a video conversion is needed, the script will send the file to a remote host and execute the ffmpeg command there
ssh_enabled = True

# Settings for ssh connection. Password can be either the password for the user or for the key file.
ssh_host = "192.168.1.1"
ssh_port = 22
ssh_user = "johndoe"
ssh_password = "supersecret"
ssh_key = "/path/to/key"

# This folder must contain the ffmpeg executable and will store the temporary video files. 
# Use an exclusive folder for this as files may be deleted or overwritten.
# This folder will be prefixed on both ffmpeg command and input and out files, so keep the name formating consistent with the OS where this will be running
ssh_folder = "C:\\ffmpeg"

ssh_ffmpeg_base_cmd = "ffmpeg.exe -loglevel error -hide_banner -y -i "
ssh_ffmpeg_video_encode = "-c:v h264_nvenc -preset llhq -zerolatency 1 -profile:v main -pix_fmt yuv420p -cq 22 -maxrate " + str(MAX_BITRATE) + " -bufsize " + str(int(MAX_BITRATE/2)) + " -vf \"scale=\'min(" + str(MAX_WIDTH) + ",iw)\':\'min(" + str(MAX_HEIGHT) + ",ih)\':force_original_aspect_ratio=decrease\""
ssh_ffmpeg_audio_encode = " -c:a aac -ac 2 -b:a 192k"
ssh_ffmpeg_middle_cmd = " -max_muxing_queue_size 1024 -map_metadata -1 -movflags +faststart"

#######################################################################
#                            Program                                  #
#######################################################################

# Paths to all valid files
paths = []

# List of conversions
commands = []

global ssh_client
global sftp_client

def setup_logger(dir, filename, debug_lvl):
    log_file = filename
    log_directory = os.path.abspath(dir)

    if not os.path.exists(log_directory):
        os.mkdir(log_directory)

    log_filePath = os.path.join(log_directory, log_file)

    if not os.path.isfile(log_filePath):
        with open(log_filePath, "w") as emptylog_file:
            emptylog_file.write('')

    logging.basicConfig(filename=log_filePath, level=debug_lvl,
                        format='%(asctime)s %(message)s')


def needs_convert(file):
    for extension in valid_extensions:
        if file.endswith(extension):
            logger.warning('Change format: ' + file)
            return True
    if file.endswith(EXT):
        stinfo = os.stat(file)
        if stinfo.st_mtime > stinfo.st_atime:
            logger.debug('Ignore: ' + file)
            return False
        logger.warning('Recode: ' + file)
        return True
    return False


def normalize_path(path):
    return path.replace('\\', '/')


def to_mp4_naming(filename):
    parts = filename.split('.')
    parts[len(parts)-1] = EXT
    output_path = '.'.join(parts)
    return output_path


def delete(path):
    logger = logging.getLogger(__name__)
    logger.info('Deleting ' + path)
    try:
        os.remove(path)
    except OSError:
        logger.exception('There was an issue deleting ' + path)

def remote_delete(path):
    logger = logging.getLogger(__name__)
    logger.info('Deleting on remote folder: ' + path)
    try:
        sftp_client.remove(path)
    except IOError:
        logger.exception('There was an issue deleting ' + path)

def move(file_from, file_to):
    logger = logging.getLogger(__name__)
    logger.info('Moving ' + file_from + ' to ' + file_to)
    try:
        os.rename(file_from, file_to)
    except OSError:
        logger.exception('There was an issue moving ' +
                         file_from + ' to ' + file_to)


def signal_handler(signum, frame):
    pass


if __name__ == '__main__':
    setup_logger(work_dir, 'media-convert.log', LOG_LEVEL)
    logger = logging.getLogger(__name__)
    # Register signals, such as CTRL + Cf
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    logger.info("######### Script Executed at " +
                time.asctime(time.localtime(time.time())))
    
    t0 = time.time()

    for base_path in watched_folders:
        base_path = normalize_path(base_path)
        logger.info('Searching for files in ' + base_path)
        t0 = time.time()
        for root, dirs, files in os.walk(base_path, topdown=True):
            dirs[:] = [d for d in dirs if d not in exclude]
            for file in files:
                if needs_convert(os.path.join(root, file)):
                    path = os.path.join(root, file)
                    paths.append(normalize_path(path))
        t1 = time.time()
        logger.info('[Directory Scan] Execution took %s seconds' % str(round(t1-t0,0)))
    logger.info('=====Scan Complete=====')
    logger.info('Total files scanned: ' + str(len(paths)))

    if len(paths) > 0:
        logger.info('Converting...')
    
    if ssh_enabled == True and len(paths) > 0:
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        try:
            ssh_client.connect(ssh_host, username=ssh_user, password=ssh_password, key_filename=ssh_key)
        except Exception as e:
            logger.error("SSH Error: " + str(e))
            ssh_enabled = False
        try:
            sftp_client = ssh_client.open_sftp()
        except Exception as e:
            logger.error("Error opening SFTP session: " + str(e))
            ssh_enabled = False
        try:
            sftp_client.chdir(ssh_folder)
        except IOError:
            logger.error("Invalid SFTP folder")
            ssh_client.close()
            ssh_enabled = False
        if ssh_enabled:
            logger.info("SSH and SFTP sessions created successfully")
        else:
            logger.warning("Disabling remote recoding due to SSH error")
    
    count = 0.0
    for path in paths:
        count += 1.0
        cur_file = normalize_path(path)
        ffmpeg_cmd = ffmpeg_base_cmd + "\"" + cur_file + "\""
        video_cmd = ' -c:v copy'
        audio_cmd = ' -c:a copy'
        need_remote = False
        redo_audio = False
        media_info = MediaInfo.parse(normalize_path(path))
        if MediaInfo.can_parse():
            for track in media_info.tracks:
                if track.track_type == 'Video':
                    if not track.bit_rate:
                        video_cmd = ffmpeg_video_encode
                        need_remote = True
                    elif not track.format.startswith(VIDEO_CODEC) or track.bit_rate > MAX_BITRATE or track.height > MAX_HEIGHT or track.width > MAX_WIDTH:
                        video_cmd = ffmpeg_video_encode
                        need_remote = True
                    elif track.format.startswith(VIDEO_CODEC) and not track.format_profile.startswith(VIDEO_PROFILE):
                        video_cmd = ffmpeg_video_encode
                        need_remote = True
                elif track.track_type == 'Audio':
                    if track.channel_s > MAX_CHANNELS or not track.format.startswith(AUDIO_CODEC):
                        redo_audio = True
                        audio_cmd = ffmpeg_audio_encode
                elif track.track_type == 'Text' and track.codec_id.startswith('S_TEXT'):
                    subname = str(track.track_id)
                    if track.language:
                        subname = track.language
                    parts = cur_file.split('.')
                    parts[len(parts)-1] = subname
                    subcount = 1
                    while os.path.isfile('.'.join(parts) + ".srt"):
                        parts[len(parts)-1] = subname + str(subcount)
                        subcount = subcount + 1
                    subfile = '.'.join(parts) + ".srt"
                    logger.info('Extracting subtitle: ' + subfile)
                    sub_cmd = "ffmpeg -loglevel error -hide_banner -i \"" + cur_file + "\" -map 0:" + str(int(track.track_id)-1) + " \"" + subfile + "\""
                    p = subprocess.Popen(sub_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    for line in p.stdout.readlines():
                        logger.error(line)
                    retval = p.wait()
                    if retval < -1 or retval > 10:
                        logger.error('Error: ffmpeg process killed, exiting')
                        sys.exit(1)
        if need_remote == True and ssh_enabled == True and JUST_CHECK == False:
            parts = cur_file.split('.')
            in_file = "in." + parts[len(parts)-1]
            out_file = "out." + EXT
            remote_infile = True
            try:
                sftp_client.lstat(in_file)
            except IOError:
                remote_infile = False
            if remote_infile:
                remote_delete(in_file)
            remote_infile = sftp_client.put(cur_file, in_file)
            if remote_infile:
                logger.info("File sent successfully")
            video_cmd = ssh_ffmpeg_video_encode
            if redo_audio:
                audio_cmd = ssh_ffmpeg_audio_encode
            ffmpeg_cmd = ssh_folder + "\\" + ssh_ffmpeg_base_cmd + "\"" + ssh_folder + "\\" + in_file + "\" " + video_cmd + audio_cmd + ssh_ffmpeg_middle_cmd + " \"" + ssh_folder + "\\" + out_file + "\""
            logger.debug("Full command: " + ffmpeg_cmd)
            try:
                stdin, stdout, stderr = ssh_client.exec_command(ffmpeg_cmd)
                retval = stdout.channel.recv_exit_status()
            except Exception as e:
                logger.error("Error running remote command: " + str(e))
            for line in stdout.readlines():
                logger.error(line)
            if retval == 0:
                logger.info('File processed successfully')
                if os.path.isfile(temp_file):
                    delete(temp_file)
                try:
                    sftp_client.get(out_file,temp_file)
                except IOError:
                    logger.error("Error downloading processed file")
                    ssh_client.close()
                    sys.exit(1)
                remote_delete(in_file)
                remote_delete(out_file)
                if DELETE:
                    delete(cur_file)
                    cur_file = to_mp4_naming(cur_file)
                    move(temp_file, cur_file)
                else:
                    if cur_file == to_mp4_naming(cur_file):
                        cur_file = cur_file + ".new"
                    cur_file = to_mp4_naming(cur_file)
                    move(temp_file, cur_file)
                stinfo = os.stat(cur_file)
                os.utime(cur_file, (stinfo.st_atime, stinfo.st_mtime+157680000))
            if retval < -1 or retval > 10:
                logger.error('Error: ffmpeg process failed remotely, exiting')
                ssh_client.close()
                sys.exit(1)
        else:
            ffmpeg_cmd = ffmpeg_cmd + video_cmd + audio_cmd + ffmpeg_middle_cmd + " \"" + temp_file + "\""

            if JUST_CHECK:
                commands.append(ffmpeg_cmd)
            else:
                if os.path.isfile(temp_file):
                    delete(temp_file)
                logger.warning('Encoding ' + cur_file)
                logger.debug(ffmpeg_cmd)
                p = subprocess.Popen(
                    ffmpeg_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                for line in p.stdout.readlines():
                    logger.error(line)
                retval = p.wait()
                logger.debug('Convert returned: ' + str(retval))
                if retval == 0:
                    logger.info('File processed successfully')
                    if DELETE:
                        delete(cur_file)
                        cur_file = to_mp4_naming(cur_file)
                        move(temp_file, cur_file)
                    else:
                        if cur_file == to_mp4_naming(cur_file):
                            cur_file = cur_file + ".new"
                        cur_file = to_mp4_naming(cur_file)
                        move(temp_file, cur_file)
                    stinfo = os.stat(cur_file)
                    os.utime(cur_file, (stinfo.st_atime, stinfo.st_mtime+157680000))
                if retval < -1 or retval > 10:
                    logger.error('Error: ffmpeg process killed, exiting')
                    sys.exit(1)

    t1 = time.time()
    logger.info('[Media Check] Execution took %s s' % str(round(t1-t0,1)))

    if JUST_CHECK:
        for cmd in commands:
            logger.info(cmd)
    sys.exit(0)
