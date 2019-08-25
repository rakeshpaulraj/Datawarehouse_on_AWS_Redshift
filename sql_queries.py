import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
CLUSTER_REGION = config.get("CLUSTER","REGION")
S3_LOG_DATA = config.get("S3","LOG_DATA")
S3_SONG_DATA = config.get("S3","SONG_DATA")
S3_LOG_JSONPATH = config.get("S3","LOG_JSONPATH")
IAM_ROLE_ARN = config.get("IAM_ROLE","ARN")

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# TRUNCATE TABLES

songplay_table_truncate = "truncate table songplays"
user_table_truncate = "truncate table users"
song_table_truncate= "truncate table songs"
artist_table_truncate = "truncate table artists"
time_table_truncate = "truncate table time"

# ------------------------------------------------------
# CREATE TABLES
# ------------------------------------------------------
# events staging table
staging_events_table_create = """
create table staging_events (
    artist varchar,
    auth varchar,
    firstName varchar,
    gender varchar,
    itemInSession integer,
    lastName varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    sessionId integer,
    song varchar,
    status varchar,
    ts varchar,
    userAgent varchar,
    userId varchar
)
diststyle auto
"""

# songs staging table
staging_songs_table_create = """ 
create table staging_songs (
    num_songs integer,
    artist_id varchar, 
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year integer
)
diststyle auto
"""


# users dimension table
# primary Key: user_id
# dist style: all 

user_table_create = """
create table users (
     user_id integer, 
     first_name varchar, 
     last_name varchar, 
     gender varchar, 
     level varchar,
     primary key(user_id)
)
diststyle all
"""

# songs dimension table
# primary Key: songs_id
# dist style: key (song_id)

song_table_create = """
create table songs (
    song_id varchar, 
    title varchar, 
    artist_id varchar, 
    year integer, 
    duration float,
    primary key(song_id)
)
distkey(song_id)
"""

# artists dimension table
# primary Key: artist_id
# dist style: key (artist_id)

artist_table_create = """
create table artists (
    artist_id varchar, 
    name varchar, 
    location varchar, 
    latitude float, 
    longitude float,
    primary key(artist_id)
)
diststyle even
"""

# time dimension table
# primary Key: start_time

time_table_create = """
create table time(
    start_time timestamp, 
    hour smallint, 
    day smallint, 
    week smallint, 
    month smallint, 
    year smallint, 
    weekday smallint,
    primary key(start_time)
)
diststyle even
"""


# songplays fact table
# primary Key: songplay_id

songplay_table_create = """
create table songplays(
    songplay_id integer identity(0,1) not null, 
    start_time timestamp not null, 
    user_id integer not null, 
    level varchar not null, 
    song_id varchar, 
    artist_id varchar, 
    session_id integer, 
    location varchar, 
    user_agent varchar,
    primary key(songplay_id)
)
distkey(song_id)
sortkey(start_time)
"""


# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
JSON {}
region '{}';
""").format(S3_LOG_DATA, IAM_ROLE_ARN, S3_LOG_JSONPATH, CLUSTER_REGION)

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
json 'auto'
region '{}';
""").format(S3_SONG_DATA, IAM_ROLE_ARN, CLUSTER_REGION)

# FINAL TABLES

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level) 
select  
      cast(userId as integer) as userId
    , firstName
    , lastName
    , gender
    , max(level) as level
from staging_events
where nvl(trim(userid), '') <> ''
group by userId, firstName, lastName, gender
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select
      song_id
    , title
    , artist_id
    , year
    , duration
from staging_songs
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
select 
      artist_id
    , artist_name
    , artist_location
    , artist_latitude
    , artist_longitude
from
(
    select
          artist_id
        , artist_name
        , artist_location
        , artist_latitude
        , artist_longitude
        , row_number() over(partition by artist_id order by year desc, artist_name) rno
    from staging_songs
) a where rno = 1
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select start_time,
extract('hour' from start_time) as hour,
extract('day' from start_time) as day,
extract('week' from start_time) as week,
extract('month' from start_time) as month,
extract('year' from start_time) as year,
extract('weekday' from start_time) as weekday
from
(
  select distinct timestamp 'epoch' + ts::bigint / 1000 * interval '1 second' as start_time
  from staging_events
  where nvl(ts, '') <> ''
) a;
""")

# songplays fact table
songplay_table_insert = (""" 
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select timestamp 'epoch' + e.ts::bigint / 1000 * interval '1 second' as start_time
    , case when nvl(userId, '') = '' then NULL else cast(e.userId as integer) end as userId
    , e.level
    , sa.song_id
    , sa.artist_id
    , e.sessionId
    , e.location
    , e.userAgent
from staging_events e
left join 
(
    select s.song_id, s.title as song_title, s.duration, a.artist_id, a.name as artist_name
    from songs s
    inner join artists a
    on s.artist_id = a.artist_id
) sa
on e.song = sa.song_title
and e.length = sa.duration
and e.artist = sa.artist_name
where e.page = 'NextSong';
""")

# QUERY LISTS


#create_table_queries = [staging_events_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

truncate_table_queries = [songplay_table_truncate, user_table_truncate, song_table_truncate, artist_table_truncate, time_table_truncate]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]