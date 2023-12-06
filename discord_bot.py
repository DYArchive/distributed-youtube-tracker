import argparse
import discord
from io import BytesIO
import json
from os.path import join, dirname, realpath
import psycopg2
import psycopg2.extras
import random
import re

localdir = dirname(realpath(__file__))

if __name__ != '__main__':
    raise Exception('Not running as main!')

match_video = re.compile(r'^(?:<)?(?:http(?:s)?://)?(?:www\.)?(?:youtu\.be/)?(?:youtube\.com/watch\?v=)?([A-Za-z0-9_-]{10}[AEIMQUYcgkosw048])(?:>)?$')
match_channel = re.compile(r'^(?:<)?(?:http(?:s)?://)?(?:www\.)?(?:youtube\.com/channel/)?(?:UC)?([A-Za-z0-9_-]{21}[AQgw])(?:>)?$')

parser = argparse.ArgumentParser()
parser.add_argument('config', help='config file')
args = parser.parse_args()

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

with open(join(localdir, args.config), 'r', encoding='utf-8') as f:
    config = load_tsv(f.read())

with open(join(localdir, 'discord_bot.helpdata'), 'r', encoding='utf-8') as f:
    helpdata = f.read()

SYNTAX_FAIL_MSG = 'Invalid syntax was used for the command. Try !tracker help'

class tracker_command:
    def __init__(self, type = None):
        self.type = type or 'command.none'
        self.arguments = {}

def process_command(message):
    command = tracker_command()
    
    # make sure this message is intended for us
    if re.match(r'^!tracker', message.content):
        command_suffix = ''.join(message.content.split('!tracker')).strip()
    else:
        return command
    
    arguments = None
    if re.match(r'^help', command_suffix):
        command.type = 'command.help'
    elif re.match(r'^stats', command_suffix):
        command.type = 'command.query_stats'
    elif re.match(r'^leaderboard', command_suffix):
        command.type = 'command.query_leaderboard'
    elif re.match(r'^channel', command_suffix):
        command.type = 'command.query_channel'
        data = re.match(r'^channel(.*)', command_suffix)[1].strip().split(' ')
        if len(data) == 1: command.arguments['channel_id'] = data[0]
        else: command.type = 'command.invalidsyntax'
    elif re.match(r'^video', command_suffix):
        command.type = 'command.query_video'
        data = re.match(r'^video(.*)', command_suffix)[1].strip().split(' ')
        if len(data) == 1: command.arguments['video_id'] = data[0]
        else: command.type = 'command.invalidsyntax'
    else:
        command.type = 'command.invalid'
    
    return command

def db_conn(config):
    return psycopg2.connect(
        database=config.get('db', 'dya_tracker'),
        host=config.get('dbhost', 'localhost'),
        port=config.get('dbport', 5432),
        user=config.get('dbuser', 'postgres'),
        password=config.get('dbpass'))

def query_leaderboard(config):
    message = '```md\nVideos leaderboard:\n'
    
    conn = db_conn(config)
    with conn.cursor() as cur:
        query = '''SELECT (SELECT name FROM contributors WHERE id = contributions_v.contributor_id),
            count(*) FROM contributions_v WHERE
            (SELECT allow_stats_queries FROM contributors WHERE id = contributions_v.contributor_id)
            GROUP BY contributor_id ORDER BY count(*) DESC LIMIT 10;'''
        cur.execute(query)
        topcontributors = cur.fetchall()
    conn.close()
    
    contribStrings = []
    longest_name = max([len(c[0]) for c in topcontributors]) + 1
    for contrib, i in zip(topcontributors, range(len(topcontributors))):
        contribStrings.append(f'\t{i+1}. {contrib[0]}:{" "*(longest_name-len(contrib[0]))}{contrib[1]:,}')
    message += '\n'.join(contribStrings)
    message += '\n```'
    return message

def query_stats(config):
    message = '```md\nStats:\n'
    
    stats = {}
    conn = db_conn(config)
    # query contributor count
    with conn.cursor(f'stats_{random.randint(0,2**20)}') as cur:
        cur.execute('SELECT count(*) FROM contributors;')
        stats['Contributors'] = f'{cur.fetchone()[0]:,}'
    
    # query total size
    with conn.cursor(f'stats_{random.randint(0,2**20)}') as cur:
        cur.execute('SELECT sum(filesize) FROM contributions_v WHERE (SELECT allow_stats_queries FROM contributors WHERE id = contributions_v.contributor_id) IS TRUE;')
        stats['Total size (TiB)'] = f'{round(cur.fetchone()[0] / 1024 / 1024 / 1024 / 1024, 2):,} TiB'
    
    # query total videos
    with conn.cursor(f'stats_{random.randint(0,2**20)}') as cur:
        cur.execute('SELECT count(*) FROM contributions_v WHERE (SELECT allow_stats_queries FROM contributors WHERE id = contributions_v.contributor_id) IS TRUE;')
        stats['Total videos'] = f'{cur.fetchone()[0]:,}'
    
    # query vid count
    with conn.cursor(f'stats_{random.randint(0,2**20)}') as cur:
        cur.execute('SELECT count(*) FROM videos;')
        stats['Unique videos'] = f'{cur.fetchone()[0]:,}'
    
    # query channel count
    with conn.cursor(f'stats_{random.randint(0,2**20)}') as cur:
        cur.execute('SELECT count(*) FROM channels;')
        stats['Unique channels'] = f'{cur.fetchone()[0]:,}'
    conn.close()
    
    message += '\n'.join(f'\t* {sn}:{" "*(20-len(sn))}{sv}' for sn, sv in stats.items()) + '\n```'
    
    return message

def query_channel(command, config):
    files = []
    message = 'unset message'
    
    og_channel_id = command.arguments['channel_id'] # to preserve `UC` prefix in bot outputs
    channel_id = match_channel.match(command.arguments['channel_id'])
    if not channel_id:
        message = f"invalid channel id '{command.arguments['channel_id']}'"
        return message, files
    
    conn = db_conn(config)
    with conn.cursor() as cur:
        # get channel in db
        query = f'''
            SELECT id, title FROM channels WHERE channel_id = '{channel_id[1]}';'''
        cur.execute(query)
        
        row = cur.fetchone()
        if not row:
            message = f'channel {channel_id[1]} not in db'
            return message, files
        channel = {'id': row[0], 'title': row[1], 'channel_id': channel_id[1]}
        
        # get list of channel contributions
        query = f'''
            SELECT id, name, verified, (SELECT note FROM contributions_c WHERE contributor_id = contributors.id AND channel_id = '{channel['id']}') FROM contributors WHERE id IN
            (SELECT contributor_id FROM contributions_c WHERE channel_id = '{channel['id']}')
            ;'''
        cur.execute(query)
        channel_maintainers = {r[0]: {'name': r[1], 'verified': r[2], 'note': r[3]} for r in cur.fetchall()}
    
    fbin = BytesIO()
    if channel_maintainers:
        fbin.write('Channel maintainers:\nUSERNAME | VERIFIED | NOTE\n'.encode('utf-8'))
        for m in channel_maintainers.values():
            fbin.write(f"{m['name']}\t{'verified' if m['verified'] else ''}\t{m['note'] if m['note'] else 'No note'}\n".encode('utf-8'))
    
    # compile all videos from channel, in db
    with conn.cursor(f'channel_{random.randint(0,2**20)}') as cur:
        query = f'''
            SELECT id, video_id, title FROM videos WHERE channel_id = {channel['id']}
        '''
        cur.execute(query)
        videos = {r[0]: {'video_id': r[1], 'title': r[2]} for r in cur.fetchall()}
    
    # filter videos by allow_channel_queries
    if len(videos) > 0:
        with conn.cursor() as cur:
            ### eww... should iterate the id list in chunks, not sure why execute_values doesn't return all rows when I do cur.fetchall()
            query = f'''
                SELECT video_id, contributor_id FROM contributions_v WHERE video_id IN ({','.join([str(v) for v in videos.keys()])}) AND (SELECT allow_channel_queries FROM contributors WHERE id = contributions_v.contributor_id) IS TRUE;
            '''
            cur.execute(query)
            
            contributors = set()
            video_contributions = {}
            for contrib in cur.fetchall():
                video_id, contributor_id = contrib
                if not video_id in video_contributions:
                    video_contributions[video_id] = set()
                contributors.update({contributor_id})
                video_contributions[video_id].update({contributor_id})
            
            # filter videos
            videos = {k: v for k, v in videos.items() if k in video_contributions}
    
    # get contributor details
    if len(videos) > 0:
        with conn.cursor() as cur:
            query = f'''
                SELECT id, name, verified, discord_id FROM contributors WHERE id IN ({','.join([str(v) for v in contributors])}) AND allow_channel_queries IS TRUE;
            '''
            cur.execute(query)
            contributors = {r[0]: {'name': r[1], 'verified': r[2], 'discord_id': r[3]} for r in cur.fetchall()}
    
    channel_videos = {}
    if len(videos) > 0:
        if channel_maintainers: fbin.write(b'\n')
        fbin.write(b'Channel videos in DB:\nID | TITLE\n')
        
        for v_id, video in videos.items():
            # write video info, and contributor details
            fbin.write(f'{video["video_id"]} - {video["title"] or "NO TITLE IN DATABASE"}\n'.encode('utf-8'))
            fbin.write(f'users: {", ".join([contributors[c]["name"] for c in video_contributions[v_id]])}\n\n'.encode('utf-8'))
        
        # write info for each contributor
        fbin.write(b'info for above contributors:\nUSERNAME | VERIFIED | DISCORD ID\n')
        fbin.write('\n'.join([f'{c["name"]} {"verified" if c["verified"] else ""} {c["discord_id"]}' for c in contributors.values()]).encode('utf-8') + b'\n')
    
    if channel_maintainers or len(videos) > 0:
        fbin.seek(0)
        files.append(discord.File(fbin, filename=f'{channel["channel_id"]}_users.txt'))
    
    message = f'found {len(videos)} videos, {len(channel_maintainers)} channel maintainers for channel `UC{channel["channel_id"]}` - `{channel["title"]}`'
    
    return message, files

def query_video(command, config):
    files = []
    message = 'unset message'
    
    video_id = match_video.match(command.arguments['video_id'])
    if not video_id:
        message = f"invalid video id '{command.arguments['video_id']}'"
        return message, files
    
    conn = db_conn(config)
    with conn.cursor() as cur:
        # get video in db
        query = f'''
            SELECT id, video_id, title, channel_id FROM videos WHERE video_id = '{video_id[1]}';'''
        cur.execute(query)
        
        row = cur.fetchone()
        if not row:
            message = f'video {video_id[1]} not in db'
            return message, files
        video = {'id': row[0], 'video_id': row[1], 'title': row[2], 'channel_id': row[3]}
        
        # get list of video contributions
        query = f'''
            SELECT contributor_id FROM contributions_v WHERE video_id = {video['id']};'''
        cur.execute(query)
        contributor_ids = [r[0] for r in cur.fetchall()]
        
        if len(contributor_ids) > 0:
            psycopg2.extras.execute_values(
                cur, '''SELECT id, name, discord_id, verified FROM contributors WHERE id IN (%s)''',
                [(cid,) for cid in contributor_ids], template='(%s)')
            contributors = {row[0]: {'n': row[1], 'd': row[2], 'v': row[3]} for row in cur.fetchall()}
        else:
            contributors = {}
        
        message = f'''{len(contributors)} users have video `{video['video_id']}` - `{video['title']  or "NO TITLE IN DATABASE"}`'''
        
        if len(contributors) > 0:
            fbin = BytesIO()
            fbin.write('USERNAME\tVERIFIED\tDISCORD ID:\n'.encode('utf-8'))
            fbin.write('\n'.join([f'''{c["n"]}\t{'verified' if c["v"] else ''}\t{c["d"]}''' for c in contributors.values()]).encode('utf-8'))
            fbin.seek(0)
            files.append(discord.File(fbin, filename=f'{video["video_id"]}_users.txt'))
    conn.close()
    
    return message, files

class scdb(discord.Client):
    global permissions
    async def on_ready(self):
        print(f'connected to discord as {self.user}')
    
    async def on_message(self, message):
        if message.author.bot or (not message.guild):
            return
        
        command = process_command(message)
        
        if command.type == 'command.invalid':
            await message.reply('Invalid command. Try !tracker help')
        elif command.type == 'command.invalidsyntax':
            await message.reply(SYNTAX_FAIL_MSG)
        elif command.type == 'command.help':
            await message.reply(helpdata)
        elif command.type == 'command.query_leaderboard':
            await message.reply(query_leaderboard(config))
        elif command.type == 'command.query_stats':
            await message.reply(query_stats(config))
        elif command.type == 'command.query_channel':
            response, files = query_channel(command, config)
            await message.reply(response, files=files)
        elif command.type == 'command.query_video':
            response, files = query_video(command, config)
            await message.reply(response, files=files)
        elif command.type != 'command.none':
            raise Exception(f'invalid command of type {command.type}')

intents = discord.Intents.default()
intents.message_content = True
client = scdb(intents=intents)
try:
    client.run(config.get('bot_token'))
except Exception as e:
    print(e)
