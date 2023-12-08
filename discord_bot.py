import aiohttp
import argparse
import discord
from io import BytesIO
import json
from os.path import join, dirname, realpath
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

async def api_call(endpoint, session, config, value=''):
    status = None
    data = ''
    try:
        async with session.get(config['dya_api_root']+endpoint+'/'+value, headers={'Authorization': config['dya_api_key']}) as response:
            return response.status, await response.text()
    except:
        return 'excepted', ''

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
    elif re.match(r'^delete-account', command_suffix):
        command.type = 'command.user_delete'
    elif re.match(r'^signup', command_suffix):
        command.type = 'command.user_signup'
        data = [s for s in re.match(r'^signup(.*)', command_suffix)[1].strip().split(' ') if s]
        if all(e in ['nostats', 'nochannels'] for e in data): command.arguments = data
        else: command.type = 'command.invalidsyntax'
    elif re.match(r'^apikey', command_suffix):
        command.type = 'command.user_request_apikey'
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

async def query_channel(command, config):
    files = []
    message = 'unset message'
    
    channel_id = match_channel.match(command.arguments['channel_id'])
    if not channel_id:
        message = f"invalid channel id '{command.arguments['channel_id']}'"
        return message, files
    
    async with aiohttp.ClientSession() as session:
        # get channel maintainers
        status, data = await api_call('channelmaintainers', session, config, channel_id[1])
        
        if status == 200:
            maintainers = json.loads(data)
        elif status == 404:
            return f'channel {channel_id[1]} does not exist in database', files
        else:
            return 'api error', files
        
        # get channel videos
        status, data = await api_call('channelvideos', session, config, channel_id[1])
        
        if status == 200:
            videos = json.loads(data)
        else:
            return 'api error', files
    
    fbin = BytesIO()
    if len(maintainers['contributions']) > 0:
        fbin.write(b'Channel maintainers:\nNAME | DISCORD ID | NOTE\n')
        for c in maintainers['contributions']:
            fbin.write(f'{c["contributor"]["name"]}\t{c["contributor"]["discord_id"]}\t{c.get("note") or "no note"}\n'.encode('utf-8'))
        fbin.write(b'\n')
    
    if len(videos['videos']) > 0:
        fbin.write(b'Channel videos in DB:\nID | TITLE\n')
        for v in videos['videos']:
            fbin.write(f'{v["id"]} - {(v["title"] or "No title in database")[:255]}\n'.encode('utf-8'))
            fbin.write(b'users with video saved:\n'+ ', '.join([
                c["name"] for c in v['contributors']
            ]).encode('utf-8') + b'\n\n')
        
        # write contributor discord ids
        contributors = {}
        for v in videos['videos']:
            contributors.update({c['discord_id']: c['name'] for c in v['contributors']})
        
        if len(contributors) > 0:
            fbin.write(b'Video contributors:\nNAME | DISCORD ID\n')
            fbin.write('\n'.join([f'{n}\t{d}' for d, n in contributors.items()]).encode('utf-8'))
    fbin.seek(0)
    
    if len(fbin.read(1)):
        fbin.seek(0)
        files.append(discord.File(fbin, filename=f'{channel_id[1]}_contributions.txt'))
    
    message = f'found {videos["count"]}{"+" if videos["count"] == 500 else ""} videos, {len(maintainers["contributions"])} channel maintainers for channel `UC{maintainers["channel"]["id"]}` - `{(maintainers["channel"]["title"] or "No title in database")[:255]}`'
    
    return message, files

async def query_video(command, config):
    files = []
    message = 'unset message'
    
    video_id = match_video.match(command.arguments['video_id'])
    if not video_id:
        message = f"invalid video id '{command.arguments['video_id']}'"
        return message, files
    
    async with aiohttp.ClientSession() as session:
        status, data = await api_call('video', session, config, video_id[1])
    
    if status == 200:
        data = json.loads(data)
        message = f'{len(data["contributions"])} users have video `{data["video"]["id"]}` - `{(data["video"]["title"] or "No title in database")[:255]}`'
        if len(data['contributions']) > 0:
            fbin = BytesIO()
            fbin.write('NAME\tDISCORD ID:\n'.encode('utf-8'))
            fbin.write('\n'.join([f'''{c["contributor"]["name"]}\t{c["contributor"]["discord_id"]}''' for c in data["contributions"]]).encode('utf-8'))
            fbin.seek(0)
            files.append(discord.File(fbin, filename=f'{data["video"]["id"]}_contributions.txt'))
    elif status == 404:
        message = f'video {video_id[1]} not in db'
    else:
        print(status)
        message = 'api call error'
    
    return message, files

async def signup_user(command, config, user):
    contributor = {
        'name': user.name,
        'discord_id': user.id,
        'allow_channel_queries': True if not 'nochannels' in command.arguments else False,
        'allow_stats_queries': True if not 'nostats' in command.arguments else False}
    
    # POST contributor
    async with aiohttp.ClientSession() as session:
        try:
            resp = await session.post(config['dya_api_root']+'signup', headers={'Authorization': config['dya_api_key']}, json=contributor)
            status = resp.status
        except:
            status = 'excepted'
        if status == 429:
            return 'api ratelimiting effective; 2 signups/minute allowed globally'
        elif status == 403:
            return 'you are already signed up'
        elif status != 200:
            return f'api error; {status} (a)'
        
        status, data = await api_call('authorize', session, config, value = str(user.id))
        if status != 200:
            return f'api error; {status} (b)'
    
    await user.send(f'api key: `{json.loads(data)["key"]}`')
    return 'signed up! I have DMed you your api key'

async def delete_user(config, user):
    async with aiohttp.ClientSession() as session:
        status, data = await api_call('authorize', session, config, value = str(user.id))
        if status == 403:
            return 'you aren\'t a registered user'
        elif status == 429:
            return 'api ratelimiting effective; 2 deletions/minute allowed globally'
        elif status != 200:
            return f'api error; {status} (a)'
        
        user_api_key = json.loads(data)['key']
        
        try:
            resp = await session.post(config['dya_api_root']+'delete_account', headers={'Authorization': user_api_key}, json={'confirm': True})
            status = resp.status
        except:
            status = 'exception'
        if status != 200:
            return f'api error; {status} (b)'
    
    return 'user successfully removed from DB'

async def fetch_apikey(config, user):
    apikey = 'error, unset'
    async with aiohttp.ClientSession() as session:
        status, data = await api_call('authorize', session, config, value = str(user.id))
        if status != 200:
            return f'api error; {status} (b)', None
    
    return 'dmed', f'api key: `{json.loads(data)["key"]}`'

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
        elif command.type == 'command.query_channel':
            response, files = await query_channel(command, config)
            await message.reply(response, files=files)
        elif command.type == 'command.query_video':
            response, files = await query_video(command, config)
            await message.reply(response, files=files)
        elif command.type == 'command.user_delete':
            response = await delete_user(config, message.author)
            await message.reply(response)
        elif command.type == 'command.user_signup':
            response = await signup_user(command, config, message.author)
            await message.reply(response)
        elif command.type == 'command.user_request_apikey':
            response, dm = await fetch_apikey(config, message.author)
            await message.reply(response)
            if dm:
                await message.author.send(dm)
        elif command.type != 'command.none':
            raise Exception(f'invalid command of type {command.type}')

intents = discord.Intents.default()
intents.message_content = True
client = scdb(intents=intents)
try:
    client.run(config.get('bot_token'))
except Exception as e:
    print(e)
