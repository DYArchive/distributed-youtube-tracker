import argparse
import json
from os import listdir, makedirs
from os.path import isfile, isdir, split, splitext, join
import re
import sys
import tarfile

is_ij = re.compile(r'.+\.info\.json$', re.IGNORECASE)
match_yt_channel_id = re.compile(r'^(?:UC)?([A-Za-z0-9_-]{21}[AQgw])$')
match_yt_video_id = re.compile(r'^([A-Za-z0-9_-]{10}[AEIMQUYcgkosw048])$')

def strip_vals(string, chars='\t\n'):
    for c in chars:
        string = string.replace(c, '')
    return string

def match_re(string, regex):
    res = regex.match(string)
    if res: return res[1]

def find_files(fdir, regex=None):
    files = []
    directories = [fdir]
    # traverse directories looking for files
    for directory in directories:
        for f in listdir(directory):
            if isfile(directory+"/"+f): files.append(directory+"/"+f)
            elif isdir(directory+"/"+f): directories.append(directory+"/"+f)
            else: print("WARNING: you shouldn't be seeing this", directory, f)
    
    if regex: # filter by expression
        files = [f for f in files if regex.match(f)]
    
    return files

def process_info_jsons(args):
    # process indir/tarball of IJs
    isTarball = False
    if isfile(args.in_path):
        isTarball = True; tar = tarfile.open(args.in_path, 'r')
        files = tar.getmembers()
    else:
        files = find_files(args.in_path, is_ij)
    
    # process/insert info-jsons in chunks of 100
    videos = {}
    channels = {}
    for fileChunk in (files[pos:pos + 100] for pos in range(0, len(files), 100)):
        for file in fileChunk:
            # get file bin
            if type(file) == tarfile.TarInfo:
                fbin = tar.extractfile(file).read()
            else:
                with open(file, 'rb') as f: fbin = f.read()
            
            # get json obj
            try: jdat=json.loads(fbin)
            except: print(f'error reading {file}'); continue
            
            # skip non-yt
            if not jdat.get('extractor') == 'youtube':
                print(f'non-youtube extractor "{jdat.get("extractor")}" from file {file}'); continue
            
            # ids
            video_id = match_re(jdat.get('id',''), match_yt_video_id)
            if not video_id: print(f'error extracting video id from {file}'); continue
            try:
                channel_id = match_re(jdat.get('channel_id') or '', match_yt_channel_id) or match_re(jdat.get('uploader_id') or '', match_yt_channel_id)
            except Exception as e:
                print(f'error parsing file {file}')
                raise e
            if not channel_id: print(f'error extracting channel id from video {video_id}, continuing anyway')
            
            # skip unlisted vids if set
            if (not args.include_unlisted) and (jdat.get('availability', 'public') != 'public'):
                print(f'skipping video {video_id}, unlisted video'); continue
            
            # titles
            if args.exclude_titles: video_title = jdat.get('title') or jdat.get('fulltitle'); video_title = strip_vals(video_title)
            else: video_title = None
            channel_title = jdat.get('channel') or jdat.get('uploader')
            if not channel_title: print(f'error extracting channel title from video {video_id}'); continue
            
            videos[video_id] = {'id': video_id, 'title': video_title, 'channel_id': 'UNSET_CHANNEL_ID', 'format_id': jdat.get('format_id'), 'filesize': jdat.get('filesize')}
            if channel_id:
                channel_id = 'UC'+channel_id
                channels[channel_id] = {'id': channel_id, 'title': channel_title}
                videos[video_id]['channel_id'] = channel_id
        print(f'{len(videos)}/{len(files)}')
    if isTarball: tar.close()
    print(f'successfully processed {len(videos)} videos')
    return videos, channels

def process_download_archive(args):
    videos = {}
    
    with open(args.in_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line: continue
            extractor, id = line.split()
            if extractor != 'youtube': print(f'skipping non-yt site {extractor}'); continue
            if not match_yt_video_id.match(id): print(f'skipping invalid youtube video id {id}'); continue
            videos[id] = {'id': id}
    
    print(f'{len(videos)} videos processed')
    
    return videos

def main(args):
    # serialize videos and channels to tsv ?
    outfile = args.outfile or ('./channels.tsv' if args.channels else './videos.tsv') # default to videos/channels.tsv
    
    # compile infojsons tarfile/indir
    if isdir(args.in_path) or splitext(args.in_path)[1] == '.tar':
        videos, channels = process_info_jsons(args)
    elif splitext(args.in_path)[1] in ['.txt', '.db', '.archive']:
        print('\n\nWARNING! I would really prefer if you compiled data from infojsons instead 👉👈\nthe DB misses out on channel names/ids and video titles\n\n')
        if args.channels: print('--channels flag does not work with downloads archive'); exit()
        videos, channels = process_download_archive(args), {}
    else: # attempt to process as download archive
        print('invalid in_path provided!'); exit()
    
    # dump vids/channels to outfile
    with open(outfile, 'w+', encoding='utf-8') as o:
        if channels:
            #compile video counts
            videocounts = {}
            for v in videos.values():
                if not v['channel_id'] in videocounts.keys():
                    videocounts[v['channel_id']] = 0
                videocounts[v['channel_id']] += 1
            
            o.write('[CHANNELS]\n')
            o.write('\t'.join(['channel_id', 'title', 'video_count', 'include (y/n, blank is y)', 'note (added to db)']) + '\n')
            
            if 'UNSET_CHANNEL_ID' in videocounts.keys():
                o.write('\t'.join(['UNSET_CHANNEL_ID', '', str(videocounts['UNSET_CHANNEL_ID']), '', '']) + '\n')
                del videocounts['UNSET_CHANNEL_ID']
            
            o.write('\n'.join([
                '\t'.join([cid, channels[cid]['title'], str(videocounts[cid]), '', ''])
                for cid in sorted(videocounts, reverse=True, key=lambda x: videocounts[x])]))
            o.write('\n')
        if videos and not args.channels:
            if channels: o.write('\n')
            o.write('[VIDEOS]\n')
            o.write('\t'.join(['video_id', 'channel_id', 'format_id (max 20 chars)', 'include (y/n, blank is y)', 'title', 'filesize']) + '\n')
            o.write('\n'.join([
                '\t'.join([v['id'], v.get('channel_id',''), v.get('format_id',''), '', v.get('title',''), str(v.get('filesize') or '')])
                for v in videos.values()]))
            o.write('\n')
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--channels', action='store_true', help='only dump list of channels (infojsons only)')
    parser.add_argument('in_path', help='TAKES INDIR OF INFOJSONS, .tar OF INFOJSONS, OR DOWNLOAD ARCHIVE FILE')
    parser.add_argument('-o', '--outfile', help='output videos/channels tsv', default=None)
    parser.add_argument('-u', '--include-unlisted', action='store_true', help='whether to collect unlisted/private/member-only videos (filters 2022+ infojsons only)')
    parser.add_argument('-t', '--exclude-titles', action='store_false', help='whether to collect video titles (infojsons only)')
    if len(sys.argv)==1:
        parser.print_help(sys.stderr); exit()
    args = parser.parse_args()
    
    if not args.outfile:
        import sys; parser.print_help(sys.stderr)
        print('\noutfile is a required arg'); exit()
    
    main(args)
