import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_PATH = config.get("S3", "LOG_DATA")
LOG_JSON_PATH = config.get("S3", "LOG_JSONPATH")
SONG_PATH = config.get("S3", "SONG_DATA")
AWS_ROLE = config.get("IAM_ROLE", "ARN")
AWS_REGION = config.get('DEST', 'REGION')

'''
    DROP TABLES
'''

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


'''
    STAGING TABLES
'''
staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist         varchar,
        auth           varchar,
        firstName      varchar,
        gender         varchar,
        itemInSession  int,
        lastName       varchar,
        length         decimal,
        level          varchar,
        location       varchar,
        method         varchar,
        page           varchar,
        registration   varchar,
        sessionID      int,
        song           varchar,
        status         int,
        ts             timestamp,
        userAgent      varchar,
        userID         int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs        int,
        artist_id        varchar,
        artist_latitude  decimal,
        artist_longitude decimal,
        artist_location  varchar,
        artist_name      varchar,
        song_id          varchar,
        title            varchar,
        duration         decimal,
        year             int  
    );
""")

'''
    DW TABLES
'''

songplay_table_create = ("""
    CREATE TABLE songplays (
        songplay_id      int             IDENTITY(0,1), 
        start_time       timestamp       REFERENCES time(start_time) sortkey, 
        user_id          int             REFERENCES users(user_id) distkey,
        level            varchar,
        song_id          varchar         REFERENCES songs(song_id), 
        artist_id        varchar         REFERENCES artists(artist_id), 
        session_id       int             NOT NULL, 
        location         text, 
        user_agent       text,
        PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id          int             distkey,
        first_name       varchar,
        last_name        varchar,
        gender           varchar,
        level            varchar,
        PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id          varchar         sortkey,
        title            varchar,
        artist_id        varchar,
        year             int,
        duration         decimal,
        
        PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id        varchar         sortkey,
        name             varchar         NOT NULL,
        location         varchar,
        latitude         decimal,
        longitude        decimal,
        
        PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time       timestamp       sortkey, 
        hour             int             NOT NULL,
        day              int             NOT NULL,
        week             int             NOT NULL,
        month            int             NOT NULL,
        year             int             NOT NULL,
        weekday          varchar         NOT NULL,
    
        PRIMARY KEY (start_time)
    );
""")


'''
    DATA FILE COPY
'''

staging_events_copy = ("""
    COPY staging_events
        FROM {source_name}
        iam_role {role_name}
        region {region_name}
        FORMAT as JSON {json_path}
        timeformat 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(
    source_name = LOG_PATH
    ,role_name = AWS_ROLE
    ,region_name = AWS_REGION
    ,json_path = LOG_JSON_PATH
)

staging_songs_copy = ("""
    COPY staging_songs
        FROM {source_name}
        iam_role {role_name}
        region {region_name}
        FORMAT as JSON 'auto'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(
    source_name = SONG_PATH
    ,role_name =  AWS_ROLE
    ,region_name = AWS_REGION
)

'''
    INSERT INTO DIMENSIONAL MODEL
'''

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT
            DISTINCT
                staging_events.ts
                ,staging_events.userId
                ,staging_events.level
                ,staging_songs.song_id
                ,staging_songs.artist_id
                ,staging_events.sessionID
                ,staging_events.location
                ,staging_events.userAgent
        FROM
            staging_events
        INNER JOIN
            staging_songs
        ON
            staging_events.song = staging_songs.title
            AND
            staging_events.artist = staging_songs.artist_name
        WHERE
            staging_events.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
        SELECT
            DISTINCT
                userId
                ,firstName
                ,lastName
                ,gender
                ,level  
        FROM
            staging_events
        WHERE
            userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
        SELECT
            DISTINCT
                song_id
                ,title
                ,artist_id
                ,year
                ,duration
        FROM
            staging_songs
        WHERE
            song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
        SELECT
            DISTINCT
                artist_id
                ,artist_name
                ,artist_location
                ,artist_latitude
                ,artist_longitude
        FROM
            staging_songs
        WHERE
            artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT
            DISTINCT
                ts
                ,EXTRACT(HOUR from ts)
                ,EXTRACT(DAY from ts)
                ,EXTRACT(WEEK from ts)
                ,EXTRACT(MONTH from ts)
                ,EXTRACT(YEAR from ts)
                ,EXTRACT(WEEKDAY from ts)
        FROM
            staging_events
        WHERE
            ts is not null;
""")

'''
    QUERY STRUCTURE
'''


create_table_queries = [staging_events_table_create, staging_songs_table_create, time_table_create
                        ,user_table_create, song_table_create, artist_table_create, songplay_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop
                      ,song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert
                        ,artist_table_insert, time_table_insert]
