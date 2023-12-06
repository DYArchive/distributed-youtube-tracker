import databases
from fastapi import FastAPI, Depends, Header, Request
from fastapi.responses import JSONResponse
import json
from os import urandom
from pydantic import BaseModel, StrictBool, StringConstraints, PositiveInt
import random
import re
from os.path import isfile, join, dirname, realpath
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.extension import Limiter
from slowapi.util import get_remote_address
import time
from typing import Optional

match_video_id = re.compile(r'^(?:<)?(?:http(?:s)?://)?(?:www\.)?(?:youtu\.be/)?(?:youtube\.com/watch\?v=)?([A-Za-z0-9_-]{10}[AEIMQUYcgkosw048])(?:>)?$')
match_channel_id = re.compile(r'^(?:<)?(?:http(?:s)?://)?(?:www\.)?(?:youtube\.com/channel/)?(?:UC)?([A-Za-z0-9_-]{21}[AQgw])(?:>)?$')

def get_api_key(request):
    return request.headers.get('Authorization') or get_remote_address(request)

limiter = Limiter(key_func=get_api_key)
app = FastAPI(root_path='/api', docs_url=None, redoc_url=None)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

configfile = join(dirname(realpath(__file__)), 'pg_creds.json')
if isfile(configfile):
    with open(configfile, 'r') as f:
        config = json.loads(f.read())
else:
    password = 'default_password'
database = databases.Database(f'postgresql+asyncpg://{config["user"]}:{config["password"]}@{config["host"]}:{config["port"]}/{config["table"]}')

def get_database():
    return database

async def verify_api_key(db, key, perm):
    await db.connect()
    row = await db.fetch_one(query=f'SELECT {perm} FROM api_keys WHERE api_key = :key', values={'key': key})
    return dict(row or {}).get(perm, False)

def generate_random(length):
    key = b''
    while len(key) < length:
        b = urandom(1)
        if re.match(b'[a-zA-Z0-9]', b):
            key += b
    return key.decode('utf-8')

@app.get('/video/{videopath:path}')
@limiter.limit('80/minute')
async def fetch_video(request: Request, videopath: str, v: str = None, db: databases.Database = Depends(get_database)):
    # assert db conn
    await db.connect()
    
    if not await verify_api_key(db, get_api_key(request), 'allow_videos_query'):
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # match video id
    videopath = v or videopath # accept ?v= query param
    vid_reg = match_video_id.match(videopath)
    if not vid_reg:
        return JSONResponse({'error': 'invalid video id'}, status_code=400)
    else:
        video_id = vid_reg[1]
    
    # pull video id, channel id, and title from db
    video = await db.fetch_one(query='''
            SELECT id,
           (SELECT channel_id FROM channels WHERE id = videos.channel_id),
           (SELECT title FROM titles_v WHERE video_id = videos.id ORDER BY time_added DESC) as title,
           (SELECT title FROM titles_c WHERE channel_id = videos.channel_id ORDER BY time_added DESC) as channel_title
            FROM videos WHERE video_id = :id''', values={'id': video_id})
    if not video:
        return JSONResponse({'error': 'video not in db'}, status_code=404)
    
    # pull video contributions from db
    contributions = await db.fetch_all(query='''
            SELECT contributor_id,
           (SELECT format_string FROM formats WHERE id = contributions_v.format_id), filesize
            FROM contributions_v WHERE video_id = :video_id''', values={'video_id': video['id']})
    contributions = {c['contributor_id']: dict(c) for c in contributions}
    
    # pull contributor info from db
    # idfk how to fetch multiple rows with `id IN ()` with this lib, I give up..
    for cid in contributions.keys():
        row = await db.fetch_one(query='SELECT id as contributor_id, name, discord_id FROM contributors WHERE id = (:id)', values={'id': cid})
        contributions[row['contributor_id']].update(dict(row))
    
    return JSONResponse({
        'contributions': [
            {
                'format_string': c.get('format_string'),
                'filesize': c.get('filesize'),
                'contributor': {
                    'name': c['name'],
                    'discord_id': c['discord_id'],
                }
            } for c in contributions.values()
        ],
        'video': {
            'id': video_id,
            'title': video['title'],
            'channel_id': video['channel_id'],
            'channel_title': video['channel_title']
        }
    }, status_code=200)

@app.get('/channelmaintainers/{channelpath:path}')
@limiter.limit('80/minute')
async def fetch_channel_maintainers(request: Request, channelpath: str, db: databases.Database = Depends(get_database)):
    # assert db conn
    await db.connect()
    
    if not await verify_api_key(db, get_api_key(request), 'allow_channelmaintainers_query'):
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # match channel id
    chn_reg = match_channel_id.match(channelpath)
    if not chn_reg:
        return JSONResponse({'error': 'invalid channel id'}, status_code=400)
    else:
        channel_id = chn_reg[1]
    
    # pull channel id from db
    channel = await db.fetch_one(query='''
        SELECT id,
        (SELECT title FROM titles_c WHERE channel_id = channels.id ORDER BY time_added DESC)
        FROM channels WHERE channel_id = :id''', values={'id': channel_id})
    if not channel:
        return JSONResponse({'error': 'channel not in db'}, status_code=404)
    
    # pull channel contributions from db
    contributions = await db.fetch_all(query='SELECT contributor_id, note FROM contributions_c WHERE channel_id = :channel_id', values={'channel_id': channel['id']})
    contributions = {c['contributor_id']: dict(c) for c in contributions}
    
    # pull contributor info from db
    for cid in contributions.keys():
        row = await db.fetch_one(query='SELECT id as contributor_id, name, discord_id, allow_channel_queries FROM contributors WHERE id = (:id)', values={'id': cid})
        contributions[row['contributor_id']].update(dict(row))
    
    return JSONResponse({
        'contributions': [
            {
                'note': c.get('note'),
                'contributor': {
                    'name': c['name'],
                    'discord_id': c['discord_id']
                }
            } for c in contributions.values() if c['allow_channel_queries']
        ],
        'channel': {
            'id': channel_id,
            'title': channel['title']
        },
    }, status_code=200)

@app.get('/channelvideos/{channelpath:path}')
@limiter.limit('80/minute')
async def fetch_channel_videos(request: Request, channelpath: str, db: databases.Database = Depends(get_database), limit: int = 500, offset: int = 0):
    if limit > 500 or limit < 1:
        return JSONResponse({'error': '`limit` allowed range is 1-500'}, status_code=400)
    elif offset < 0:
        return JSONResponse({'error': '`offset` must not be negative'}, status_code=400)
    
    # assert db conn
    await db.connect()
    
    if not await verify_api_key(db, get_api_key(request), 'allow_channelvideos_query'):
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # match channel id
    chn_reg = match_channel_id.match(channelpath)
    if not chn_reg:
        return JSONResponse({'error': 'invalid channel id'}, status_code=400)
    else:
        channel_id = chn_reg[1]
    
    # pull channel id from db
    channel = await db.fetch_one(query='''
        SELECT id, (SELECT title FROM titles_c WHERE channel_id = channels.id ORDER BY time_added DESC) FROM channels WHERE channel_id = :id''',
        values={'id': channel_id})
    if not channel:
        return JSONResponse({'error': 'channel not in db'}, status_code=404)
    
    # pull list of videos from `videos`
    rows = await db.fetch_all(query='''
            SELECT
                id,
                video_id,
                (SELECT title FROM titles_v WHERE video_id = videos.id ORDER BY time_added DESC LIMIT 1) as title
            FROM videos
            WHERE channel_id = :cid
            AND (
                SELECT TRUE FROM contributions_v WHERE video_id = videos.id AND (
                    SELECT allow_channel_queries FROM contributors WHERE id = contributions_v.contributor_id
                ) IS TRUE LIMIT 1
            ) IS TRUE
            ORDER BY id ASC LIMIT :limit OFFSET :offset
        ''', values={'cid': channel['id'], 'limit': limit, 'offset': offset})
    videos = {r['id']: {'v_id': r['video_id'], 'title': r['title'], 'contributors': {}} for r in rows}
    
    # fetch contributions / contributors
    if len(videos) > 0:
        rows = await db.fetch_all(query=f'''
                SELECT
                    video_id,
                    contributor_id,
                    (SELECT name FROM contributors WHERE id = contributions_v.contributor_id) as contributor_name,
                    (SELECT discord_id FROM contributors WHERE id = contributions_v.contributor_id) as contributor_discord_id
                FROM contributions_v
                WHERE video_id IN ({','.join([str(v) for v in videos.keys()])})
                AND (SELECT allow_channel_queries FROM contributors WHERE id = contributions_v.contributor_id) IS TRUE;
            ''')
    else:
        rows = []
    
    # filter contributions without any contributors that allow channel queries
    contributions = {}
    for r in rows:
        videos[r['video_id']]['contributors'].update({r['contributor_id']: {'name': r['contributor_name'], 'discord_id': int(r['contributor_discord_id'])}})
    videos = {k: v for k, v in videos.items() if len(v['contributors']) > 0}
    
    return JSONResponse({
        'count': len(videos),
        'nextOffset': offset + len(videos) if len(videos) == limit else None,
        'channel': {
            'id': channel_id,
            'title': channel['title']
        },
        'videos': [
            {
                'id': v['v_id'],
                'title': v.get('title'),
                'contributors': [
                    {
                        'name': co['name'],
                        'discord_id': co['discord_id']
                    }
                    for co in v['contributors'].values()
                ]
            }
        for v in videos.values()
    ]
    }, status_code=200)

@app.post('/submit_channels')
@limiter.limit('80/minute')
async def submit_channels(request: Request, db: databases.Database = Depends(get_database)):
    # assert db conn
    await db.connect()
    
    contributor_id = await verify_api_key(db, get_api_key(request), 'allow_submit_contributions')
    if type(contributor_id) != int:
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # validate body
    try:
        jsonDat = await request.json()
    except json.decoder.JSONDecodeError:
        return JSONResponse({'error': 'malformed body'}, status_code=400)
    
    if list(jsonDat.keys()) != ['channels']:
        return JSONResponse({'error': 'missing `channels` key or invalid keys present'}, status_code=400)
    
    if len(jsonDat['channels']) > 500:
        return JSONResponse({'error': 'maxiumum of 500 channels per api call'}, status_code=400)
    
    # process channels
    fields = {'id', 'title', 'note'}
    channels = {}
    missing_fields = {f: 0 for f in fields}
    for c in jsonDat['channels']:
        if not 'id' in c:
            return JSONResponse({'error': 'channel missing `id` key'}, status_code=400)
        for field in fields:
            if not field in c:
                missing_fields[field] += 1
        
        # test channel ids
        c_id = match_channel_id.match(c['id'])
        if not c_id:
            return JSONResponse({'error': f'invalid channel id {c["id"]}'}, status_code=400)
        c_id = c_id[1]
        
        channels[c_id] = {
            'id': c_id,
            'title': c.get('title') or None,
            'note': c.get('note') or None}
    
    channel_id_lookup = {}
    if len(channels) > 0:
        # insert channel ids
        await db.execute_many(query='INSERT INTO channels (channel_id) VALUES (:cid) ON CONFLICT DO NOTHING', values=[{'cid': cid} for cid in channels.keys()])
        
        # fetch channel ids
        for c in channels.keys():
            row = await db.fetch_one(query='SELECT id as dbid, channel_id FROM channels WHERE channel_id = :cid', values={'cid': c})
            channel_id_lookup[row.channel_id] = row.dbid
    
        # insert channel titles
        titled_channels = [
            {
                'ta': time.time(),
                'chid': channel_id_lookup[c['id']],
                'cnid': contributor_id,
                'title': c.get('title')
            } for c in channels.values() if c.get('title')]
        if titled_channels:
            await db.execute_many(query='INSERT INTO titles_c (time_added, channel_id, contributor_id, title) VALUES (:ta, :chid, :cnid, :title) ON CONFLICT DO NOTHING', values=titled_channels)
    
    # insert contributions_c
    await db.execute_many(query='INSERT INTO contributions_c (channel_id, contributor_id, note) VALUES (:chid, :cnid, :note) ON CONFLICT DO NOTHING',values=[
            {
                'chid': channel_id_lookup[c['id']],
                'cnid': contributor_id,
                'note': c['note']
            } for c in channels.values()
        ])
    
    jresp = {
        'warning': {'missing_field_counts': {k: v for k, v in missing_fields.items() if v > 0}},
        'success': True}
    if not jresp['warning']['missing_field_counts']:
        del jresp['warning']
    
    return JSONResponse(jresp, status_code=200)

@app.post('/submit_videos')
@limiter.limit('80/minute')
async def submit_videos(request: Request, db: databases.Database = Depends(get_database)):
    # assert db conn
    await db.connect()
    
    contributor_id = await verify_api_key(db, get_api_key(request), 'allow_submit_contributions')
    if type(contributor_id) != int:
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # validate body
    try:
        jsonDat = await request.json()
    except json.decoder.JSONDecodeError:
        return JSONResponse({'error': 'malformed body'}, status_code=400)
    
    if list(jsonDat.keys()) != ['videos']:
        return JSONResponse({'error': 'missing `videos` key or invalid keys present'}, status_code=400)
    
    if len(jsonDat['videos']) > 500:
        return JSONResponse({'error': 'maxiumum of 500 videos per api call'}, status_code=400)
    
    # process videos
    fields = {'id', 'title', 'channel_id', 'channel_title', 'filesize', 'format_id'}
    videos = {}
    channels = {}
    missing_fields = {f: 0 for f in fields}
    for v, i in zip(jsonDat['videos'], range(len(jsonDat['videos']))):
        if not 'id' in v:
            return JSONResponse({'error': f'video at position `{i}` is missing `id` key'}, status_code=400)
        for field in fields:
            if not field in v:
                missing_fields[field] += 1
        
        # test video and channel ids
        v_id = match_video_id.match(v['id'])
        if not v_id:
            return JSONResponse({'error': f'invalid video id {v["id"]}'}, status_code=400)
        v_id = v_id[1]
        
        if 'channel_id' in v:
            c_id = match_channel_id.match(v['channel_id'])
            if not c_id:
                return JSONResponse({'error': f'invalid channel id `{v["channel_id"]}`'}, status_code=400)
            c_id = c_id[1]
        else:
            c_id = None
        
        # assert filesize is int (ik this is so dumb and I should be using pydantic)
        if v.get('filesize') and type(v.get('filesize')) == str:
            if not v['filesize'].isdigit():
                return JSONResponse({'error': 'filesize should be int, digit str, or null'}, status_code=400)
        
        videos[v_id] = {
            'id': v_id,
            'title': v.get('title') or None,
            'channel_id': c_id or None,
            'filesize': int(v.get('filesize') or 0) or None,
            'format_id': v.get('format_id') or None}
        
        if (c_id not in channels) and c_id:
            channels[c_id] = {'id': c_id}
        if v.get('channel_title') and c_id:
            channels[c_id]['title'] = v.get('channel_title')
    
    channel_id_lookup = {}
    if len(channels) > 0:
        # insert channel ids
        await db.execute_many(query='INSERT INTO channels (channel_id) VALUES (:cid) ON CONFLICT DO NOTHING', values=[{'cid': cid} for cid in channels.keys()])
        
        # fetch channel ids
        for c in channels.keys():
            row = await db.fetch_one(query='SELECT id as dbid, channel_id FROM channels WHERE channel_id = :cid', values={'cid': c})
            channel_id_lookup[row.channel_id] = row.dbid
        
        # insert channel titles
        titled_channels = [
            {
                'ta': time.time(),
                'chid': channel_id_lookup[c['id']],
                'cnid': contributor_id,
                'title': c.get('title')
            } for c in channels.values() if c.get('title')]
        if titled_channels:
            await db.execute_many(query='INSERT INTO titles_c (time_added, channel_id, contributor_id, title) VALUES (:ta, :chid, :cnid, :title) ON CONFLICT DO NOTHING', values=titled_channels)
    
    format_ids = {v.get('format_id') for v in videos.values() if v.get('format_id')}
    format_id_lookup = {}
    if format_ids:
        # insert format ids
        await db.execute_many(query='INSERT INTO formats (format_string) VALUES (:fs) ON CONFLICT DO NOTHING', values=[{'fs': fs[:255]} for fs in format_ids])
        
        # fetch format ids
        for fs in format_ids:
            row = await db.fetch_one(query='SELECT id FROM formats WHERE format_string = :fs', values={'fs': fs[:255]})
            format_id_lookup[fs] = row.id
    
    # insert video ids
    await db.execute_many(query='INSERT INTO videos (video_id, channel_id) VALUES (:vid, :cid) ON CONFLICT DO NOTHING', values=[
        {'vid': v['id'], 'cid': channel_id_lookup.get(v['channel_id'])} for v in videos.values()])
    
    # fetch video ids
    video_id_lookup = {}
    for vid in videos.keys():
        row = await db.fetch_one(query='SELECT id FROM videos WHERE video_id = :vid', values={'vid': vid})
        video_id_lookup[vid] = row.id
    
    # insert video titles
    titled_videos = {vk: vv for vk, vv in videos.items() if vv['title']}
    if titled_videos:
        await db.execute_many(query='INSERT INTO titles_v (time_added, video_id, contributor_id, title) VALUES (:ta, :vid, :cnid, :title) ON CONFLICT DO NOTHING', values=[
            {
                'ta': time.time(),
                'vid': video_id_lookup[v['id']],
                'cnid': contributor_id,
                'title': v['title']
            } for v in titled_videos.values()
        ])
    
    # insert contributions_v
    await db.execute_many(query='INSERT INTO contributions_v (video_id, contributor_id, format_id, filesize) VALUES (:vid, :cid, :fid, :size) ON CONFLICT DO NOTHING',values=[
            {
                'vid': video_id_lookup[v['id']],
                'cid': contributor_id,
                'fid': format_id_lookup.get(v['format_id']),
                'size': v['filesize']
            } for v in videos.values()
        ])
    
    jresp = {
        'warning': {'missing_field_counts': {k: v for k, v in missing_fields.items() if v > 0}},
        'success': True}
    if not jresp['warning']['missing_field_counts']:
        del jresp['warning']
    
    return JSONResponse(jresp, status_code=200)

@app.get('/my_channels')
@limiter.limit('80/minute')
async def query_contributor_channels(request: Request, db: databases.Database = Depends(get_database), limit: int = 500, offset: int = 0):
    if limit > 500 or limit < 1:
        return JSONResponse({'error': '`limit` allowed range is 1-500'}, status_code=400)
    elif offset < 0:
        return JSONResponse({'error': '`offset` must not be negative'}, status_code=400)
    
    # assert db conn
    await db.connect()
    
    contributor_id = await verify_api_key(db, get_api_key(request), 'allow_submit_contributions')
    if type(contributor_id) != int:
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    rows = await db.fetch_all(query='''
        SELECT
            (SELECT channel_id FROM channels WHERE id = contributions_c.channel_id) as channel_id,
            (SELECT title FROM titles_c WHERE channel_id = contributions_c.channel_id) as channel_title,
            note
        FROM contributions_c WHERE contributor_id = :cnid ORDER BY contributions_c.channel_id LIMIT :limit OFFSET :offset''', values={
        'cnid': contributor_id,
        'limit': limit,
        'offset': offset})
    
    return JSONResponse({
        'count': len(rows),
        'nextOffset': offset + len(rows) if len(rows) == limit else None,
        'channels': [dict(r) for r in rows]
        }, status_code=200)

@app.get('/my_videos')
@limiter.limit('80/minute')
async def query_contributor_videos(request: Request, db: databases.Database = Depends(get_database), limit: int = 500, offset: int = 0):
    if limit > 500 or limit < 1:
        return JSONResponse({'error': '`limit` allowed range is 1-500'}, status_code=400)
    elif offset < 0:
        return JSONResponse({'error': '`offset` must not be negative'}, status_code=400)
    
    # assert db conn
    await db.connect()
    
    contributor_id = await verify_api_key(db, get_api_key(request), 'allow_submit_contributions')
    if type(contributor_id) != int:
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    rows = await db.fetch_all(query='''
        SELECT
            (SELECT video_id FROM videos WHERE id = contributions_v.video_id) as id,
            (SELECT title FROM titles_v WHERE video_id = contributions_v.video_id) as title,
            (SELECT (SELECT channel_id FROM channels WHERE id = videos.channel_id) FROM videos WHERE id = contributions_v.video_id) as channel_id,
            (SELECT (SELECT title FROM titles_c WHERE channel_id = videos.channel_id) FROM videos WHERE id = contributions_v.video_id) as channel_title,
            (SELECT format_string FROM formats WHERE id = contributions_v.format_id) as format_id,
            filesize
        FROM contributions_v WHERE contributor_id = :cnid ORDER BY contributions_v.video_id LIMIT :limit OFFSET :offset''', values={
        'cnid': contributor_id,
        'limit': limit,
        'offset': offset})
    
    return JSONResponse({
        'count': len(rows),
        'nextOffset': offset + len(rows) if len(rows) == limit else None,
        'videos': [dict(r) for r in rows]
        }, status_code=200)

@app.delete('/my_channels/{channelpath:str}')
@limiter.limit('80/minute')
async def delete_contributor_channel(request: Request, channelpath: str, db: databases.Database = Depends(get_database)):
    # match channel id
    chn_reg = match_channel_id.match(channelpath)
    if not chn_reg:
        return JSONResponse({'error': 'invalid channel id'}, status_code=400)
    else:
        channel_id = chn_reg[1]
    
    # assert db conn
    await db.connect()
    
    contributor_id = await verify_api_key(db, get_api_key(request), 'allow_submit_contributions')
    if type(contributor_id) != int:
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    await db.execute(query='DELETE FROM contributions_c WHERE channel_id = (SELECT id FROM channels WHERE channel_id = :chid) AND contributor_id = :cnid', values={
        'chid': channel_id,
        'cnid': contributor_id})
    
    return JSONResponse({'success': True}, status_code=200)

@app.delete('/my_videos/{videopath:str}')
@limiter.limit('80/minute')
async def delete_contributor_video(request: Request, videopath: str, v: str = None, db: databases.Database = Depends(get_database)):
    # match video id
    videopath = v or videopath # accept ?v= query param
    vid_reg = match_video_id.match(videopath)
    if not vid_reg:
        return JSONResponse({'error': 'invalid video id'}, status_code=400)
    else:
        video_id = vid_reg[1]
    
    # assert db conn
    await db.connect()
    
    contributor_id = await verify_api_key(db, get_api_key(request), 'allow_submit_contributions')
    if type(contributor_id) != int:
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    await db.execute(query='DELETE FROM contributions_v WHERE video_id = (SELECT id FROM videos WHERE video_id = :vid) AND contributor_id = :cnid', values={
        'vid': video_id,
        'cnid': contributor_id})
    
    return JSONResponse({'success': True}, status_code=200)

@app.post('/delete_all')
@limiter.limit('2/minute')
async def delete_all_contributions(request: Request, db: databases.Database = Depends(get_database)):
    # assert db conn
    await db.connect()
    
    contributor_id = await verify_api_key(db, get_api_key(request), 'allow_submit_contributions')
    if type(contributor_id) != int:
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # validate body
    try:
        jsonDat = await request.json()
    except json.decoder.JSONDecodeError:
        return JSONResponse({'error': 'malformed body'}, status_code=400)
    
    if not jsonDat.get('confirm') == True:
        return JSONResponse({'error': '`confirm` is not `true`'}, status_code=403)
    
    # delete videos
    await db.execute(query='DELETE FROM contributions_v WHERE contributor_id = :cnid', values={
        'cnid': contributor_id})
    
    # delete channels
    await db.execute(query='DELETE FROM contributions_c WHERE contributor_id = :cnid', values={
        'cnid': contributor_id})
    
    return JSONResponse({'success': True}, status_code=200)

@app.post('/signup')
@limiter.limit('2/minute')
async def create_contributor(request: Request, db: databases.Database = Depends(get_database)):
    # assert db conn
    await db.connect()
    
    # check perms
    if not await verify_api_key(db, get_api_key(request), 'allow_create_user'):
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # validate body
    try:
        jsonDat = await request.json()
    except json.decoder.JSONDecodeError:
        return JSONResponse({'error': 'malformed body'}, status_code=400)
    
    # verify value types/presence of keys
    notnull = {'allow_channel_queries': bool, 'allow_stats_queries': bool, 'name': str, 'discord_id': int}
    for k in notnull.keys():
        if jsonDat.get(k) == None:
            return JSONResponse({'error': f'{k} cannot be null'}, status_code=400)
        elif type(jsonDat.get(k)) != notnull[k]:
            return JSONResponse({'error': f'{k} cannot be type {type(jsonDat[k])}'}, status_code=400)
    
    # verify length of name
    if type(jsonDat['name']) != str or len(jsonDat['name']) > 60:
        return JSONResponse({'error': 'name must be 60 characters or less'}, status_code=400)
    
    contributor = {
        'allow_channel_queries': jsonDat['allow_channel_queries'],
        'allow_stats_queries': jsonDat['allow_stats_queries'],
        'name': jsonDat['name'],
        'discord_id': str(jsonDat['discord_id'])}
    
    # check db for user
    inDb = await db.fetch_one(query='SELECT id FROM contributors WHERE discord_id = :did',values={'did': contributor['discord_id']})
    if inDb:
        return JSONResponse({'error': 'contributor already exists'}, status_code=403)
    
    # insert contributor
    x = await db.execute(
        query='''INSERT INTO contributors (allow_channel_queries, allow_stats_queries, name, discord_id) VALUES
            (:allow_channel_queries, :allow_stats_queries, :name, :discord_id)''',
        values=contributor)
    
    contributor['discord_id'] = int(contributor['discord_id']) # set discord id back to int
    
    return JSONResponse(contributor, status_code=200)

@app.get('/authorize/{discord_id:str}')
@limiter.limit('2/minute')
async def authorize_contributor(request: Request, discord_id: int, db: databases.Database = Depends(get_database)):
    # assert db conn
    await db.connect()
    
    # check perms
    if not await verify_api_key(db, get_api_key(request), 'allow_create_user_api_keys'):
        return JSONResponse({'error': 'insufficient permissions'}, status_code=401)
    
    # get contributor id
    row = await db.fetch_one(query='SELECT id FROM contributors WHERE discord_id = :did', values={'did': str(discord_id)})
    row = dict(row or {})
    if row:
        contributor_id = row['id']
    else:
        return JSONResponse({'error': 'user does not exist'}, status_code=403)
    
    # insert api key
    await db.execute(query='''
        INSERT INTO api_keys (application, api_key, allow_submit_contributions, allow_videos_query, allow_channelmaintainers_query, allow_channelvideos_query)
        VALUES (:application, :api_key, :allow_submit_contributions, TRUE, TRUE, TRUE) ON CONFLICT DO NOTHING''',
        values={'application': f'discord_user_{discord_id}', 'api_key': generate_random(64), 'allow_submit_contributions': contributor_id})
    
    # fetch api_key
    row = await db.fetch_one(query='SELECT api_key FROM api_keys WHERE allow_submit_contributions = :cid', values={'cid': contributor_id})
    row = dict(row or {})
    
    return JSONResponse({
        'key': row.get('api_key'),
        'scope': [
            '/video/{videopath:path}', '/channelvideos/{channelpath:path}',
            '/channelmaintainers/{channelpath:path}', '/submit_channels',
            '/submit_videos', '/my_videos', '/my_channels', '/my_videos/{videopath:str}',
            '/my_channels/{channelpath:str}', '/delete_all']
        }, status_code=200)

@app.on_event('shutdown')
async def shutdown_event():
    await database.disconnect()
