CREATE DATABASE dya_tracker;

/* psql connect to db */
\c dya_tracker

CREATE TABLE contributors (
    id SERIAL PRIMARY KEY,
	allow_channel_queries BOOL NOT NULL,
	allow_stats_queries BOOL NOT NULL,
	verified BOOL DEFAULT FALSE NOT NULL,
    videos_last_updated INT,
    channels_last_updated INT,
	name VARCHAR(60) NOT NULL,
    discord_id VARCHAR(20) UNIQUE NOT NULL,
    other_contact_info JSONB
);

CREATE TABLE formats (
	id SERIAL PRIMARY KEY,
	format_string VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE contributions_v (
    video_id INT NOT NULL,
    contributor_id INT NOT NULL,
	format_id INT,
	filesize BIGINT,
    UNIQUE(video_id, contributor_id)
);
CREATE INDEX contributions_contributor_id_idx ON contributions_v (contributor_id);

CREATE TABLE contributions_c (
    channel_id INT NOT NULL,
    contributor_id INT NOT NULL,
	note TEXT,
    UNIQUE(channel_id, contributor_id)
);
CREATE INDEX contributions_channel_id_idx ON contributions_c (channel_id);

CREATE TABLE channels (
    id SERIAL PRIMARY KEY NOT NULL,
    channel_id CHAR(22) UNIQUE NOT NULL,
    title VARCHAR(255)
);

CREATE TABLE videos (
    id SERIAL PRIMARY KEY NOT NULL,
    video_id CHAR(11) UNIQUE NOT NULL,
    channel_id INT, /* id of row in channels table */
    title VARCHAR(255) /* should only be 100 chars */
);
CREATE INDEX videos_channel_id_idx ON videos (channel_id);
