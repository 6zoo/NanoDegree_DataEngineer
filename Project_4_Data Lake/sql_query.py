song_table_sql = ("""
                SELECT
                    , song_id
                    , title
                    , artist_id
                    , year
                    , duration
                FROM song
                GROUP BY 1, 2, 3, 4, 5
            """)


artist_table_sql = ("""
                SELECT
                    artist_id
                    , name
                    , location
                    , lattitude
                    , longitude
                FROM artists
                GROUP BY 1, 2, 3, 4, 5
            """)


filtered_log_sql = ("""
                SELECT *
                    , cast(ts/1000 as Timestamp) as timestamp
                from staging_events
                where page = 'NextSong'
            """)

user_table_sql = ("""
                SELECT
                    user_id
                    , first_name
                    , last_name
                    , gender
                    , level
                FROM users
                GROUP BY 1, 2, 3, 4, 5
            """)
    
time_table_sql = ("""
                SELECT
                    distinct timestamp as start_time, 
                    hour(timestamp) as hour, 
                    day(timestamp) as day, 
                    weekofyear(timestamp) as week, 
                    month(timestamp) as month, 
                    year(timestamp) as year, 
                    weekday(timestamp) as weekday
                FROM staging_events
            """)


songplays_table_sql = """
                SELECT
                songplay_id
                , a.start_time
                , a.user_id
                , a.level
                , b.song_id
                , b.artist_id
                , a.session_id
                , a.location
                , a.user_agent
                , year(a.timestamp) as year
                , month(a.timestamp) as month
                FROM staging_events inner as a join songs as b
                    on songs.title = staging_events.song
"""
