import argparse
import json
from os import makedirs
from os.path import isfile, isdir, split
import psycopg2
import psycopg2.extras
import re
import time

match_yt_channel_id = re.compile(r'^(?:UC)?([A-Za-z0-9_-]{21}[AQgw])$')
match_yt_video_id = re.compile(r'^([A-Za-z0-9_-]{10}[AEIMQUYcgkosw048])$')

def load_tsv(data):
    tsv = {}
    for line in data.splitlines():
            if not line: continue
            try:
                k, v, t = line.split('\t')
            except:
                print(line)
                exit()
            if t == 'str': tsv[k] = v
            elif t == 'int': tsv[k] = int(v)
            elif t == 'csv': tsv[k] = v.split(',')
            else: raise Exception(f'unknown val type {t}')
    return tsv

def echo_msg(msg, fh):
    fh.write(f'{msg}\n'); print(msg)

def convert_user_input(val, allowed):
    if val in allowed.keys() or not allowed.keys():
        return allowed[val] if allowed.keys() else val
    else:
        print(f'error! invalid input given! ({"/".join(list(allowed.keys()))})')
        return None

def retry_user_input(message, allowed_vals={'y': True, 'n': False}):
    val = None
    while val == None:
        val = convert_user_input(input(message), allowed=allowed_vals)
    return val

def get_contributor_id(cur, args, logh):
    cur.execute(f"SELECT id FROM contributors WHERE discord_id = '{int(args.discord_id)}'")
    contributor_id = cur.fetchone()
    
    if not contributor_id:
        print('Discord id not in contributors table!')
        username = retry_user_input('input username: ', allowed_vals={})[:60]
        print(f'username is {username}')
        allow_channel_q = retry_user_input('allow channel queries? (y/n): ')
        allow_stats_q = retry_user_input('allow stats queries? (y/n): ')
        verified = retry_user_input('mark user as verified? (y/n): ')
        
        contact_info = {}
        
        note = retry_user_input('user note: ', allowed_vals={}) or None
        if note: contact_info['note'] = note
        
        if retry_user_input('input contact info? (y/n): '):
            for platform in ['email', 'disord_handle', 'twitter_handle', 'youtube_id', 'reddit_user', 'github_user']:
                val = retry_user_input(f'{platform}:', allowed_vals={}) or None
                if val: contact_info[platform] = val
        
        cur.execute(
            f'''INSERT INTO contributors (name, allow_channel_queries, allow_stats_queries, verified, videos_last_updated, discord_id, other_contact_info) VALUES (%s, %s, %s, %s, %s, %s, %s)''',
            (username, allow_channel_q, allow_stats_q, verified, 0, args.discord_id, None if not contact_info else json.dumps(contact_info)))
        
        cur.execute(f"SELECT id FROM contributors WHERE discord_id = '{args.discord_id}';")
        contributor_id = cur.fetchone()
        
        echo_msg(f'inserted new contributor {contributor_id}!', logh)
    
    return contributor_id[0]

def match_re(string, regex):
    res = regex.match(string)
    if res: return res[1]

def insert_maintained_channels(channels, cur, args, contributor_id, logh, chunksize=100):
    # insert channels
    channel_id_lookup = {}
    affected_channel_rows = 0
    channel_ids = list(channels.keys())
    for i in range(0, len(channel_ids), chunksize):
        chunk = channel_ids[i:i+chunksize]
        
        # insert channels
        psycopg2.extras.execute_values(
            cur, '''INSERT INTO channels (channel_id, title) VALUES %s
            ON CONFLICT (channel_id) DO UPDATE SET title = COALESCE(EXCLUDED.title, channels.title)''',
            [(ci, channels[ci]['t']) for ci in chunk if ci != None])
        affected_channel_rows += cur.rowcount
        
        # fetch channel ids
        psycopg2.extras.execute_values(
            cur, '''SELECT channel_id, id FROM channels WHERE channel_id IN (%s)''',
            [(c,) for c in chunk], template='(%s)')
        channel_id_lookup.update({row[0]: row[1] for row in cur.fetchall()})
    echo_msg(f'inserted/updated {affected_channel_rows} channels', logh)
    
    # insert channel contributions
    affected_channel_contribution_rows = 0
    for i in range(0, len(channel_ids), chunksize):
        chunk = channel_ids[i:i+chunksize]
        psycopg2.extras.execute_values(
            cur, '''INSERT INTO contributions_c (channel_id, contributor_id, note) VALUES %s
            ON CONFLICT (channel_id, contributor_id) DO UPDATE SET note = COALESCE(EXCLUDED.note, contributions_c.note)''',
            [(channel_id_lookup[ci], contributor_id, channels[ci]['n'],) for ci in chunk])
        affected_channel_contribution_rows += cur.rowcount
    echo_msg(f'inserted {affected_channel_contribution_rows} channel contribution rows', logh)
    
    # update contributor channels_last_updated
    cur.execute(f'UPDATE contributors SET channels_last_updated = {time.time()} WHERE id = {contributor_id}')

def insert_videos_and_channels(videos, channels, cur, args, contributor_id, logh, chunksize=100):
    # insert channels
    channel_id_lookup = {}
    affected_channel_rows = 0
    channel_ids = list(channels.keys())
    for i in range(0, len(channel_ids), chunksize):
        chunk = channel_ids[i:i+chunksize]
        
        # insert channels
        psycopg2.extras.execute_values(
            cur, '''INSERT INTO channels (channel_id, title) VALUES %s
            ON CONFLICT (channel_id) DO UPDATE SET title = COALESCE(EXCLUDED.title, channels.title)''',
            [(ci, channels[ci]['t']) for ci in chunk if ci != None])
        affected_channel_rows += cur.rowcount
        
        # fetch channel ids
        psycopg2.extras.execute_values(
            cur, '''SELECT channel_id, id FROM channels WHERE channel_id IN (%s)''',
            [(c,) for c in chunk], template='(%s)')
        channel_id_lookup.update({row[0]: row[1] for row in cur.fetchall()})
    echo_msg(f'inserted/updated {affected_channel_rows} channels', logh)
    
    # insert videos
    video_id_lookup = {}
    affected_video_rows = 0
    video_ids = list(videos.keys())
    for i in range(0, len(video_ids), chunksize):
        chunk = video_ids[i:i+chunksize]
        
        # insert videos
        psycopg2.extras.execute_values(
            cur, '''INSERT INTO videos (video_id, channel_id, title) VALUES %s
            ON CONFLICT (video_id) DO UPDATE SET (title, channel_id) = (COALESCE(EXCLUDED.title, videos.title), COALESCE(EXCLUDED.channel_id, videos.channel_id))''',
            [(vi, channel_id_lookup.get(videos[vi]['c']), videos[vi]['t']) for vi in chunk if vi != None])
        affected_video_rows += cur.rowcount
        
        # fetch video ids
        psycopg2.extras.execute_values(
            cur, '''SELECT video_id, id FROM videos WHERE video_id IN (%s)''',
            [(vi,) for vi in chunk], template='%s')
        video_id_lookup.update({row[0]: row[1] for row in cur.fetchall()})
    echo_msg(f'inserted/updated {affected_video_rows} videos', logh)
    
    # compile and insert format_ids
    format_ids = list({v['f'] for v in videos.values() if v['f'] != None})
    psycopg2.extras.execute_values(
        cur, '''INSERT INTO formats (format_string) VALUES %s ON CONFLICT DO NOTHING''',
        [(f,) for f in format_ids], template='(%s)')
    
    # fetch db format ids
    psycopg2.extras.execute_values(
        cur, '''SELECT format_string, id FROM formats WHERE format_string IN (%s)''',
        [(f,) for f in format_ids], template='%s')
    format_id_lookup = {row[0]: row[1] for row in cur.fetchall()}
    
    # insert video contributions
    affected_video_contribution_rows = 0
    for i in range(0, len(video_ids), chunksize):
        chunk = video_ids[i:i+chunksize]
        try:
            psycopg2.extras.execute_values(
                cur, '''INSERT INTO contributions_v (video_id, contributor_id, format_id, filesize) VALUES %s
                ON CONFLICT DO NOTHING''',
                [(video_id_lookup[vi], contributor_id, format_id_lookup.get(videos[vi]['f']), videos[vi]['s']) for vi in chunk])
        except:
            print(chunk)
            exit()
        affected_video_contribution_rows += cur.rowcount
    echo_msg(f'inserted {affected_video_contribution_rows} video contribution rows', logh)
    
    # update contributor videos_last_updated
    cur.execute(f'UPDATE contributors SET videos_last_updated = {time.time()} WHERE id = {contributor_id}')

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

def main(args, config):
    logfile = args.log_file_fmt.format(args.discord_id)
    if not isdir(split(logfile)[0]):
        makedirs(split(logfile)[0])
    
    conn = psycopg2.connect(
        host = config.get('dbhost'),
        port = config.get('dbport'),
        database = config.get('db'),
        user = config.get('dbuser'),
        password = config.get('dbpass'))
    
    # get contributor id
    with conn.cursor() as cur, open(logfile, 'a+', encoding='utf-8') as logh:
        contributor_id = get_contributor_id(cur, args, logh)
        echo_msg(f'fetched contributor_id: {contributor_id}', logh)
    
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
    
    with conn.cursor() as cur, open(logfile, 'a+', encoding='utf-8') as logh:
        if args.channels:
            echo_msg('now inserting maintained channels:', logh)
            insert_maintained_channels(channels, cur, args, contributor_id, logh)
        else:
            echo_msg('now inserting videos/channels:', logh)
            insert_videos_and_channels(videos, channels, cur, args, contributor_id, logh)
        
        conn.commit()
        echo_msg('finished!', logh)
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-con', '--config', help='config tsv', default='./db_config.tsv')
    parser.add_argument('-i', '--in-file', help='input videos tsv')
    parser.add_argument('-d', '--discord-id', help='discord id for contributor')
    parser.add_argument('--channels', action='store_true', help='insert all channels from tsv as maintained channels')
    parser.add_argument('-l', '--log-file-fmt', default='./logs/{}.log', help='output fmt for log files (curly braces are discord id), default: ./{}.log')
    args = parser.parse_args()
    
    if (not args.discord_id) or (not args.in_file):
        import sys; parser.print_help(sys.stderr)
        print('\ndiscord id and infile are required args'); exit()
    
    if not isfile(args.config):
        raise Exception(f'no config file found for "{args.config}"')
    
    with open(args.config, 'r+') as f:
        config = load_tsv(f.read())
    
    main(args, config)
