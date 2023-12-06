CREATE DATABASE dya_tracker;

/* psql connect to db */
\c dya_tracker

CREATE TABLE contributors (
    id SERIAL PRIMARY KEY,
	allow_channel_queries BOOL NOT NULL,
	allow_stats_queries BOOL NOT NULL,
	name VARCHAR(60) NOT NULL,
    discord_id VARCHAR(20) UNIQUE NOT NULL
);

CREATE TABLE formats (
	id SERIAL PRIMARY KEY,
	format_string TEXT NOT NULL UNIQUE
);

CREATE TABLE contributions_c (
    channel_id INT NOT NULL,
    contributor_id INT NOT NULL,
	note TEXT,
    UNIQUE(channel_id, contributor_id)
);
CREATE INDEX contributions_channel_id_idx ON contributions_c (channel_id);

CREATE TABLE contributions_v (
    video_id INT NOT NULL,
    contributor_id INT NOT NULL,
	format_id INT,
	filesize BIGINT,
    UNIQUE(video_id, contributor_id)
);
CREATE INDEX contributions_contributor_id_idx ON contributions_v (contributor_id);

CREATE TABLE titles_c (
	time_added INT,
	channel_id INT,
	contributor_id INT,
	title TEXT,
	UNIQUE (channel_id, title)
);
CREATE INDEX titles_c_id_idx ON titles_c (channel_id);

CREATE TABLE titles_v (
	time_added INT,
	video_id INT,
	contributor_id INT,
	title TEXT,
	UNIQUE (video_id, title)
);
CREATE INDEX titles_v_id_idx ON titles_v (video_id);

CREATE TABLE channels (
    id SERIAL PRIMARY KEY NOT NULL,
    channel_id CHAR(22) UNIQUE NOT NULL
);

CREATE TABLE videos (
    id SERIAL PRIMARY KEY NOT NULL,
    video_id CHAR(11) UNIQUE NOT NULL,
    channel_id INT /* id of row in channels table */
);
CREATE INDEX videos_channel_id_idx ON videos (channel_id);

CREATE TABLE api_keys(
	application TEXT,
	api_key CHAR(64) PRIMARY KEY NOT NULL,
	allow_videos_query BOOL DEFAULT FALSE,
	allow_channelmaintainers_query BOOL DEFAULT FALSE,
	allow_channelvideos_query BOOL DEFAULT FALSE,
	allow_submit_contributions INT DEFAULT NULL UNIQUE,
	allow_create_user BOOL DEFAULT FALSE,
	allow_create_user_api_keys BOOL DEFAULT FALSE
);

CREATE USER dya_tracker_api;
GRANT SELECT ON channels, videos, contributions_c, contributions_v, titles_c, titles_v, contributors, formats, api_keys TO dya_tracker_api;
GRANT INSERT ON channels, videos, contributions_c, contributions_v, titles_c, titles_v, contributors, formats, api_keys TO dya_tracker_api;
GRANT UPDATE ON videos, titles_c, titles_v, contributors, contributions_c, contributions_v TO dya_tracker_api;
GRANT UPDATE ON channels_id_seq, videos_id_seq, contributors_id_seq, formats_id_seq TO dya_tracker_api;
GRANT DELETE ON contributions_c, contributions_v TO dya_tracker_api;
ALTER USER dya_tracker_api WITH PASSWORD 'default_password';
