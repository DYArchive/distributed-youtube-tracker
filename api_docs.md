# why aren't you using pydantic / sqlalchemy to generate docs automatically?  
lazy  

# notes
* channel IDs are stripped of their `UC` prefix
* channel/video ids are automatically parsed from url if a url is passed
* provide your api key in the `Authorization` request header

# User-accessible endpoints  

## GET `/api/video/{video}`  
fetch video id/title/channel/list of contributions  

## GET `/api/channelmaintainers/{channel}`  
fetch channel id/title/list of channel maintainers  

## GET `/api/channelvideos/{channel}`
fetch channel id/title/list of channel's videos/list of video contributors
note: videos only contributed by a user who doesn't allow channel queries will not have the video appear

## POST `/api/submit_channels`
submit channel ids/titles/notes and mark user as maintainer of channels
limit of 500 channels per request
body:
```json
[
	{
		"id": "uAXFkgsw1L7xaCfnd5JJOw",
		"title": "Rick Astley",
		"note": "started archiving in 2019"
	}
]
```

## POST `/api/submit_videos`
submit video ids/titles/channels/size/format and mark user as contributor of videos
limit of 500 videos per request
body:
```json
[
	{
		"id": "dQw4w9WgXcQ",
		"title": "Rick Astley - Never Gonna Give You Up (Official Music Video)",
		"channel_id": "uAXFkgsw1L7xaCfnd5JJOw",
		"channel_title": "Rick Astley",
		"filesize": 157988945,
		"format_id": "616-dash+251-dash"
	}
]
```

## GET `/api/my_videos?limit=500&offset=0`
fetch list of contributed videos, supports pagination
limit of 500 videos per request
use `nextOffset` variable for pagination

## GET `/api/my_channels?limit=500&offset=0`
fetch list of maintained channels, supports pagination
limit of 500 channels per request
use `nextOffset` variable for pagination

## DELETE `/api/my_videos/{video}`
delete video from your video contributions

## DELETE `/api/my_channels/{channel}`
delete video from your maintained channels

## POST `/api/delete_all`
delete all video/channel contributions (doesn't remove ids or titles from database)
body:
```json
{"confirm": true}
```

# Private endpoints

## POST `/api/signup`
register discord user
body:
```json
{
	"discord_id": 0,
	"name": "audrey",
	"allow_channel_queries": true,
	"allow_stats_queries": true
}
```

## GET `/api/authorize/{discord_id}`
create/fetch api key for user from database

## GET `/api/stats`
query database for stats
