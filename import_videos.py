import argparse
import json
from os import makedirs
from os.path import isfile, isdir, split
import re
import requests
import sys
import time

match_yt_channel_id = re.compile(r'^(?:UC)?([A-Za-z0-9_-]{21}[AQgw])$')
match_yt_video_id = re.compile(r'^([A-Za-z0-9_-]{10}[AEIMQUYcgkosw048])$')

def echo_msg(msg, fh):
    fh.write(f'{msg}\n'); print(msg)

def match_re(string, regex):
    res = regex.match(string)
    if res: return res[1]

def insert_maintained_channels(channels, args, logh, chunksize=500):
    channel_ids = list(channels.keys())
    for i in range(0, len(channel_ids), chunksize):
        print(f'inserting channels {min(i+chunksize, len(channel_ids))}/{len(channel_ids)}')
        chunk = channel_ids[i:i+chunksize]
        flattened_channels = [
            {
                'id': id,
                'title': channels[id]['t'],
                'note': channels[id]['n']
            }
            for id in chunk]
        
        while True:
            try:
                resp = requests.post(args.api_root_url + '/submit_channels', headers={'Authorization': args.api_key}, json={'channels': flattened_channels})
                status = resp.status_code
            except Exception as e:
                status = str(e)
            
            if status == 200:
                break
            elif status == 401:
                raise Exception(f'invalid api key passed')
            elif status == 429:
                print('429 ratelimiting.. retrying')
                time.sleep(5)
            else:
                print(f'bad status {status}.. retrying')
                time.sleep(1)

def insert_videos_and_channels(videos, channels, args, logh, chunksize=500):
    video_ids = list(videos.keys())
    for i in range(0, len(video_ids), chunksize):
        print(f'inserting videos {min(i+chunksize, len(video_ids))}/{len(video_ids)}')
        chunk = video_ids[i:i+chunksize]
        flattened_videos = [
            {
                'id': id,
                'title': videos[id]['t'],
                'channel_id': videos[id]['c'],
                'channel_title': channels[videos[id]['c']]['t'],
                'format_id': videos[id]['f'],
                'filesize': videos[id]['s']
            }
            for id in chunk]
        
        while True:
            try:
                resp = requests.post(args.api_root_url + '/submit_videos', headers={'Authorization': args.api_key}, json={'videos': flattened_videos})
                status = resp.status_code
            except Exception as e:
                status = str(e)
            
            if status == 200:
                break
            elif status == 401:
                raise Exception(f'invalid api key passed')
            elif status == 429:
                print('429 ratelimiting.. retrying')
                time.sleep(5)
            else:
                print(f'bad status {status}.. retrying')
                time.sleep(1)
    
def parse_line(line, header=None):
    row = [c.strip() for c in line.strip().split('\t')]
    if header:
        row = {header[ci]: c for c, ci in zip(row, range(len(row))) if ci < len(header)}
    else:
        row = [c.split()[0] for c in row]
    
    return row

def cast_str_as_val(string, rtype=str):
    return rtype(string) if string else None

def main(args):
    logfile = args.log_file_fmt.format(args.api_key)
    if not isdir(split(logfile)[0]):
        makedirs(split(logfile)[0])
    
    # parse infile
    fields = []
    currentmode = None
    skipped_channels = set()
    channels, videos = {}, {}
    with open(args.in_file, 'r', encoding='utf-8') as f, open(logfile, 'a+', encoding='utf-8') as logh:
        while line := f.readline():
            strippedline = line.strip()
            if strippedline in ['', '[CHANNELS]', '[VIDEOS]']: # list lookup is faster for small strings/number of elements
                if not strippedline: continue
                elif strippedline == '[CHANNELS]': currentmode = 'c'
                elif strippedline == '[VIDEOS]': currentmode = 'v'
                echo_msg(f'now processing {strippedline} portion', logh)
                fields = parse_line(f.readline().strip()) # update header
            else:
                if currentmode == 'c':
                    row = parse_line(line, fields)
                    if row.get('include') == 'n' or (not row.get('channel_id')) or row.get('channel_id') == 'UNSET_CHANNEL_ID':
                        echo_msg(f'skipping channel {row.get("channel_id")} ({row.get("title")})', logh); skipped_channels.update({row.get('channel_id')}); continue
                    
                    channels[row['channel_id']] = {
                        't': cast_str_as_val(row.get('title')),
                        'c': cast_str_as_val(row.get('video_count'), rtype=int),
                        'n': cast_str_as_val(row.get('note'))}
                elif currentmode == 'v':
                    row = parse_line(line, fields)
                    if row.get('include') == 'n' or (not row.get('video_id')):
                        echo_msg(f'skipping video {row.get("video_id")} ({row.get("title")})', logh); continue
                        continue
                    
                    videos[row['video_id']] = {
                        't': cast_str_as_val(row.get('title')),
                        'f': cast_str_as_val(row.get('format_id')),
                        'c': cast_str_as_val(row.get('channel_id')),
                        's': cast_str_as_val(row.get('filesize'), rtype=int)}
    
        echo_msg(f'processed {len(channels)} channels, {len(videos)} videos', logh)
    
    # remove videos from skipped channels
    # doesn't strip videos without channel ids because an unset channel id == None/null
    videos = {vi: v for vi, v in videos.items() if not v['c'] in skipped_channels}
    
    with open(logfile, 'a+', encoding='utf-8') as logh:
        if args.channels:
            echo_msg('now inserting maintained channels:', logh)
            insert_maintained_channels(channels, args, logh)
        else:
            echo_msg('now inserting videos:', logh)
            insert_videos_and_channels(videos, channels, args, logh)
        echo_msg('finished!', logh)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--in-file', help='input videos tsv')
    parser.add_argument('-k', '--api-key', help='dya tracker api key', default=None)
    parser.add_argument('--api-root-url', help='dya tracker api url', default='https://dya-t-api.strangled.net/api')
    parser.add_argument('--channels', action='store_true', help='insert all channels from tsv as MAINTAINED channels')
    parser.add_argument('-l', '--log-file-fmt', default='./logs/{}.log', help='output fmt for log files (curly braces are discord id), default: ./{}.log')
    if len(sys.argv)==1:
        parser.print_help(sys.stderr); exit()
    args = parser.parse_args()
    
    if (not args.api_key) or (not args.in_file):
        import sys; parser.print_help(sys.stderr)
        print('\napi key and infile are required args'); exit()
    
    main(args)
