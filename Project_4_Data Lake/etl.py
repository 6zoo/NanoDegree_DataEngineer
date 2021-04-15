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
    """
    creating spark session
    """
    ### create spark session using SparkSession
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()

    ### set file output committed to _temporary folder. it makes writing files to S3 faster
        ## ref) https://stackoverflow.com/a/42834182/4549682

    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return spark


def process_song_data(spark, input_data, output_data):
    """
    Load data from song_data file which stored in s3
    and extract columns and create tables using written sql query from sql_query.py
    and writing songs and artists table into parquet
    and the files will be loaded on S3 bucket
    
    Parameters
    ----------
    spark: session
          spark session that has been created
    input_data: path
          path to the song_data s3 bucket.
    output_data: path
          path to where the parquet files will be written.
    """
    
    # get filepath to song data file
    song_data = input_data + 'song_data/A/A/A/*.json'
    print('starting read of song_data json files: ' + str(datetime.now()))

    # read song data file
    df = spark.read.json(song_data)
    print('loading of song_data files complete: ' + str(datetime.now()))
    
    # extract columns to create songs table
    df.createOrReplaceTempView('songs')
    songs_table = spark.sql(song_table_sql)

    # write songs table to parquet files partitioned by year and artist
    print('writing songs table to S3: ' + str(datetime.now()))
    songs_table.write.partitionBy('year', 'artist_id')\
                    .parquet(os.path.join(output_data, 'songs/songs.parquet'), 'overwrite')
    print('write of songs table to S3 complete: ' + str(datetime.now()))

    
    
    # extract columns to create artists table
    df.createOrReplaceTempView('artists')
    artists_table = spark.sql(artist_table_sql)
    
    # write artists table to parquet files
    print('writing artists table to S3: ' + str(datetime.now()))
    artists_table.write.parquet(os.path.join(output_data, 'artists/artists.parquet'), 'overwrite')
    print('write of artists table to S3 complete: ' + str(datetime.now()))


def process_log_data(spark, input_data, output_data):
    """
    Load data from log_data file which stored in s3
    and extract columns and create three tables using log_date and song_data with using sql_query.py
    and writing users, time and songplays table into parquet
    and the files will be loaded on S3 bucket
    
    Parameters
    ----------
    spark: session
          spark session that has been created
    input_data: path
          path to the log_data s3 bucket.
    output_data: path
          path to where the parquet files will be written.
    """
    
    # get filepath to log data file
    log_data = input_data + 'log_data/2018/11/2018-11-12-events.json'
    
    # read log data file
    print('reading log data from S3: ' + str(datetime.now()))
    df = spark.read.json(log_data)
    print('loading of log data from S3 complete: ' + str(datetime.now()))
    
    # filter by actions for song plays
    df.createOrReplaceTempView("staging_events")
    df = spark.sql(filtered_log_sql)


    
    # extract columns for users table   
    user_table = spark.sql(user_table_sql).dropDuplicates(['user_id', 'level'])
    
    # write users table to parquet files
    print('writing users table to S3: ' + str(datetime.now()))
    user_table.write.parquet(os.path.join(output_data, 'users/users.parquet'), 'overwrite')
    print('loading of users table to S3: ' + str(datetime.now()))

    
    
    # create timestamp column from original timestamp column
    ### I used SQL join clause to get timestamp from staging_events table instead of using udf(maybe use labmda str functions)
    time_table = spark.sql(time_table_sql)

    print('writing time table to S3 partitioned by year and month: ' + str(datetime.now()))
    time_table.write.partitionBy('year', 'month')\
                .parquet(os.path.join(output_data, 'time/time.parquet'), 'overwrite')
    print('write of time table to S3 complete: ' + str(datetime.now()))
    
    
    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "/songs/songs.parquet")
    song_df.createOrReplaceTempView("songs")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql(songplays_table_sql)

    # write songplays table to parquet files partitioned by year and month
    print('writing songplays table to S3 partitioned by year and month: ' + str(datetime.now()))
    songplays_table.write.partitionBy('year', 'month') \
                .parquet(os.path.join(output_data, 'songplays/songplays.parquet'), 'overwrite')
    print('write of songplays table to S3 complete: ' + str(datetime.now()))


def main():    
    """
    Perform the following roles:
    1.) Get or create a spark session.
    1.) Read the song and log data from s3.
    2.) Extract and Transform them into tables
    3.) Write and Load the parquet files on s3.
    """

    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://your_bucket_name/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
