import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSON_PATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")
REGION = config.get("S3", "REGION")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INTEGER,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INTEGER,
        song                VARCHAR,
        status              INTEGER,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INTEGER 
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs           INTEGER,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INTEGER
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays
    (
        songplay_id BIGINT   PRIMARY KEY,
        start_time timestamp NOT NULL sortkey distkey,
        user_id INTEGER      NOT NULL,
        level VARCHAR,
        song_id VARCHAR      NOT NULL,
        artist_id VARCHAR    NOT NULL,
        session_id INTEGER,
        location VARCHAR,
        user_agent VARCHAR
    )
""")

user_table_create = ("""
    CREATE TABLE users
    (
        user_id INTEGER NOT NULL sortkey PRIMARY KEY,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    )
""")

song_table_create = ("""
    CREATE TABLE songs
    (
        song_id INTEGER NOT NULL sortkey PRIMARY KEY,
        title VARCHAR NOT NULL,
        artist_id VARCHAR NOT NULL,
        year INTEGER NOT NULL,
        duration FLOAT
    )
""")

artist_table_create = ("""
    CREATE TABLE artists
    (
        artist_id VARCHAR NOT NULL sortkey PRIMARY KEY,
        name VARCHAR NOT NULL,
        location VARCHAR,
        latitude FLOAT,
        longitude FLOAT
    )
""")

time_table_create = ("""
    CREATE TABLE time
    (
        start_time timestamp NOT NULL sortkey PRIMARY KEY,
        hour INTEGER NOT NULL,
        day INTEGER NOT NULL,
        week INTEGER NOT NULL,
        month INTEGER NOT NULL,
        year INTEGER NOT NULL,
        weekday INTEGER NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
        FROM {}
        iam_role '{}'
        FORMAT AS JSON {}
        REGION {}
        TIMEFORMAT AS 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSON_PATH, REGION)

staging_songs_copy = ("""
    COPY staging_songs
        FROM {}
        iam_role '{}'
        FORMAT AS JSON 'auto'
        REGION {};
""").format(SONG_DATA, ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT stge.userId,
                        stge.level,
                        stge.songId,
                        stge.artistId,
                        stge.sessionId,
                        stge.location,
                        stge.userAgent
        FROM staging_events as stge
            INNER JOIN staging_songs stgs
                ON (stge.song = stgs.title AND stge.artist = stgs.artist_name)
        AND stge.page == 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users ( user_id, first_name, last_name, gender, level)
        SELECT distinct user_id, first_name, last_name, gender, level
        FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO songs ( song_id, title, artist_id, year, duration)
        SELECT distinct  song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id is not null
""")

artist_table_insert = ("""
    INSERT INTO artists ( artist_id, name, location, latitude, longitude)
        SELECT distinct  song_id, title, artist_id, year, duration
        FROM staging_songs
        WHERE song_id is not null
""")

time_table_insert = ("""
    INSERT INTO time ( start_time, hour, day, week, month, year, weekday)
        SELECT distinct  start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), extract(month from start_time), extract(year from start_time), extract(weekday from start_time)
        FROM songplay
        WHERE start_time is not null
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
