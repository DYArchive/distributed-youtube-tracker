# distributed youtube tracker
py scripts / db schema / discord bot for tracking youtube archives  

# py scripts
compile_videos.py -  
takes input download-archive, infojsons-tarfile, or directory containing infojsons  
and compiles to a tsv of channels and videos (or just channels)!  

filter_videos_file.py -  
takes input tsv file, and filter out channels/videos excluded by editing the tsv  

import_videos.py -  
takes tsv files and imports them into DB, prompts for contact/etc for new users  

# bot commands
 * !tracker help
	* `print this help message`
 
 * !tracker video {video id}
	* `query DB for users who have video saved`
 
 * !tracker channel {channel id}
	* `query DB for channel maintainers and for saved channel videos`

 * !tracker signup {nochannels} {nostats}
	* signup for an api key to contribute to the tracker
	* optional arguments: nochannels and nostats; e.g. !tracker signup nochannels nostats

 * !tracker delete_account
	* delete your account and contributions from the tracker

# scope of allow_channel_queries and allow_stats_queries flags
## stats are not yet implemented
allow_stats_queries toggles whether you appear in:  
* total videos in `!tracker stats`  
* total size in `!tracker stats`  
* appearing in leaderboard in `!tracker leaderboard`  

allow_channel_queries toggles whether your archived videos will show up in channel queries,  
you won't show up as a user who has the video saved unless they do a video query, and if you are the only user then that video won't show up in the channel query  

# notes
bot requires following discord perms:  
* read messages  
* send messages  
* send files  

# todo
* add user video/channel wishlist
* add statistics/leaderboard queries

# api architecture
nginx -> gunicorn -> fastapi -> postgres
