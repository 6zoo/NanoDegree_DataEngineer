import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from sql_query import song_table_sql, artist_table_sql, filtered_log_sql, user_table_sql, time_table_sql, songplays_table_sql


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    ### create spark session using SparkSession
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)
    songs_table.createOrReplaceTempView('songs')

    # extract columns to create songs table
    songs_table = spark.sql(song_table_sql)

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id')\
                    .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')

    
    
    # extract columns to create artists table
    artists_table.createOrReplaceTempView('artists')
    artists_table = spark.sql(artist_table_sql)
    
    # write artists table to parquet files
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data/*/*/*-events.json'

    # read log data file
    df = spark.read.json(log_data)
    df.createOrReplaceTempView("staging_events")
    
    # filter by actions for song plays
    df = spark.sql(filtered_log_sql)


    
    # extract columns for users table    
    user_table.createOrReplaceTempView('users')
    user_table = spark.sql(user_table_sql)
    
    # write users table to parquet files
    user_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')

    
    
    # create timestamp column from original timestamp column
    ### I used SQL join clause to get timestamp from staging_events table instead of using udf(maybe use labmda str functions)
    time_table = spark.sql(time_table_sql)
    time_table.write.partitonBy('year', 'month')\
                .parquet(os.path.join(output_date, 'time/time.parquet'), 'overwrite')
    
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs/songs.parquet")
    song_df = createOrReplaceTempView('songs')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplays_table_sql)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy('year', 'month') \
                .parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')



def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
