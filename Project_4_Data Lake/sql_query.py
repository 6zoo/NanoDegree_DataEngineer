song_table_sql = ("""
                SELECT
                    song_id
                    , title as song_title
                    , artist_id
                    , year
                    , duration
                FROM songs
                GROUP BY 1, 2, 3, 4, 5
            """)


artist_table_sql = ("""
                SELECT
                    artist_id
                    , artist_name
                    , artist_location
                    , artist_latitude
                    , artist_longitude
                FROM artists
                GROUP BY 1, 2, 3, 4, 5
            """)


filtered_log_sql = ("""
                SELECT *
                    , cast(ts/1000 as Timestamp) as timestamp
                FROM staging_events
                WHERE page = 'NextSong'
            """)

user_table_sql = ("""
                SELECT
                    userId as user_id
                    , firstName as first_name
                    , lastName as last_name
                    , gender
                    , level
                FROM staging_events
                GROUP BY 1, 2, 3, 4, 5
            """)
    
time_table_sql = ("""
                SELECT
                    cast(ts/1000 as Timestamp) as start_time, 
                    hour(cast(ts/1000 as Timestamp)) as hour, 
                    day(cast(ts/1000 as Timestamp)) as day, 
                    weekofyear(cast(ts/1000 as Timestamp)) as week, 
                    month(cast(ts/1000 as Timestamp)) as month, 
                    year(cast(ts/1000 as Timestamp)) as year, 
                    weekday(cast(ts/1000 as Timestamp)) as weekday
                FROM staging_events
                GROUP BY 1, 2, 3, 4, 5, 6, 7
            """)


songplays_table_sql = ("""
                SELECT
                    a.ts as start_time
                    , a.userId as user_id
                    , a.level
                    , b.song_id
                    , b.artist_id
                    , a.sessionId
                    , a.location
                    , a.userAgent as user_agent
                    , year(cast(a.ts/1000 as Timestamp)) as year
                    , month(cast(a.ts/1000 as Timestamp)) as month 
                FROM staging_events as a 
                    INNER JOIN songs as b on a.song = b.song_title
            """)