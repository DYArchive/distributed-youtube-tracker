import argparse

def echo_msg(msg, fh):
    fh.write(f'{msg}\n'); print(msg)

def parse_line(line, header=None):
    row = [c.strip() for c in line.strip().split('\t')]
    if header:
        row = {header[ci]: c for c, ci in zip(row, range(len(row))) if ci < len(header)}
    else:
        row = [c.split()[0] for c in row]
    
    return row

def cast_str_as_val(string, rtype=str):
    try:
        return rtype(string) if string else None
    except Exception as e:
        print(string)
        raise e

def main(args):
    logfile = './filter_videos.log'
    with open(logfile, 'a+', encoding='utf-8') as logh, open(args.infile, 'r', encoding='utf-8') as ifh, open(args.outfile, 'w+', encoding='utf-8') as ofh:
        echo_msg('loading input file', logh)
        
        filtered_videos = 0
        filtered_channels = 0
        skipped_channels = set()
        
        # parse channels
        ifh.seek(0)
        fields = []
        currentmode = None
        while line := ifh.readline():
            strippedline = line.strip()
            if strippedline in ['', '[CHANNELS]', '[VIDEOS]']:
                if not strippedline: continue
                elif strippedline == '[CHANNELS]':
                    currentmode = 'c'; ofh.write(line)
                    line = ifh.readline(); ofh.write(line)
                    fields = parse_line(line.strip()) # update header
                elif strippedline == '[VIDEOS]': currentmode = 'v'
            elif currentmode == 'c':
                row = parse_line(line, fields)
                if row.get('include') == 'n' or (not row.get('channel_id')):
                    echo_msg(f'skipping channel {row.get("channel_id")} ({row.get("title")})', logh)
                    skipped_channels.update({row.get('channel_id')})
                    filtered_channels += 1; continue
                else:
                    ofh.write(line)
        # parse videos
        ifh.seek(0)
        fields = []
        currentmode = None
        while line := ifh.readline():
            strippedline = line.strip()
            if strippedline in ['', '[CHANNELS]', '[VIDEOS]']:
                if not strippedline: continue
                elif strippedline == '[CHANNELS]': currentmode = 'c'
                elif strippedline == '[VIDEOS]':
                    currentmode = 'v'; ofh.write(line)
                    line = ifh.readline(); ofh.write(line)
                    fields = parse_line(line.strip()) # update header
            elif currentmode == 'v':
                row = parse_line(line, fields)
                if row.get('include') == 'n' or (not row.get('video_id')) or ((row.get('channel_id') or 'UNSET_CHANNEL_ID') in skipped_channels):
                    echo_msg(f'skipping video {row.get("video_id")} ({row.get("title")})', logh)
                    filtered_videos += 1; continue
                else:
                    ofh.write(line)
        echo_msg(f'filtered {filtered_videos} videos and {filtered_channels} channels', logh)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', help='input videos/channels tsv')
    parser.add_argument('outfile', help='output filtered tsv')
    parser.add_argument('--channels', action='store_true', help='only copy channel rows')
    args = parser.parse_args()
    
    main(args)
