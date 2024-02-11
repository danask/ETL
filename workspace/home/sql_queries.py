import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

##########################
# DROP TABLES
##########################

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

##########################
# CREATE TABLES
##########################

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    event_id INT IDENTITY(0, 1) PRIMARY KEY,
    artist VARCHAR(max),
    auth VARCHAR(max),
    firstName VARCHAR(max),
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR(max),
    length DOUBLE PRECISION,
    level VARCHAR(max),
    location VARCHAR(max),
    method VARCHAR(max),
    page VARCHAR(300),
    registration DOUBLE PRECISION,
    sessionId INT,
    song VARCHAR(max),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(max),
    userId VARCHAR(max)
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    artist_id VARCHAR(max),
    artist_latitude DOUBLE PRECISION,
    artist_location VARCHAR(max),
    artist_longitude DOUBLE PRECISION,
    artist_name VARCHAR(max),
    duration DOUBLE PRECISION,
    num_songs INT,
    song_id VARCHAR(max),
    title VARCHAR(max),
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
    songplay_id int IDENTITY(0, 1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id VARCHAR(max) NOT NULL,
    level VARCHAR(max),
    song_id VARCHAR(max) NOT NULL,
    artist_id VARCHAR(max) NOT NULL,
    session_id BIGINT NOT NULL,
    location VARCHAR(max),
    user_agent VARCHAR(max)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id VARCHAR(max) PRIMARY KEY,
    first_name VARCHAR(max),
    last_name VARCHAR(max),
    gender CHAR(1),
    level VARCHAR(max) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
    song_id VARCHAR(max) PRIMARY KEY,
    title VARCHAR(max),
    artist_id VARCHAR(max) NOT NULL, -- FOREIGN KEY (artist_id) REFRENCES artists(artis_id)
    year INT,
    duration DOUBLE PRECISION               
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
    artist_id VARCHAR(max) PRIMARY KEY,
    name VARCHAR(max),
    location VARCHAR(max),
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")


##########################
# STAGING TABLES
##########################

staging_events_copy = ("""copy staging_events from {} iam_role {} json {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""copy staging_songs from {} iam_role {} json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))


##########################
# FINAL TABLES
##########################

songplay_table_insert = ("""
INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' as start_time,
    se.userId,
    se.level,
    ss.song_id as song_id,
    ss.artist_id as artist_id,
    se.sessionId as session_id,
    se.location as location,
    se.userAgent as user_agent
FROM staging_events se, staging_songs ss
WHERE se.page = 'NextSong'
and se.song = ss.title
and se.artist = ss.artist_name
and se.length = ss.duration
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song (song_id, title, artist_id, year, duration)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id, name, location, latitude, longitude)
SELECT DISTINCT
    artist_id,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(dayofweek from start_time) as weekday
FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
