# Udacity NanoDegree - Data Engineer
## Project.4 Data Lake

--

### Introduction
 As a Data Engineer of "Sparkify" which is a start-up and operates Music streaming Services, I built an ETL pipeline for a data lake hosted on S3 and load data from S3, process the data into analytics tables using AWS Elastic Map Reduce which runs on Spark, and load them back into S3.

### How to run this project
    1. Open Terminal
    2. Set your configuration information, and save it as dwh.cfg in the root folder
       1) You should create your EMR Cluster and IAM role
       2) You should create your S3 bucket that same region as ERM cluster
    4. Run etl.py (!python etl.py) - It processes and loads data into tables created by 2 procedure.

### DataSets
     1. Song data
     2. Log data 

### Database Schema
 - Database schema of this project was built in "Snow Flake" modeling.
 - Consists of 1 Fact Table, and 4 Dimension Tables

#### 1) Staging Tables
 - staging_events

#### 2) Fact Table
 - songplays - records in event data associated with song plays i.e. records with page NextSong - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### 3) Dimension Tables
 - users - users in the app - user_id, first_name, last_name, gender, level
 - songs - songs in music database - song_id, title, artist_id, year, duration
 - artists - artists in music database - artist_id, name, location, lattitude, longitude
 - time - timestamps of records in songplays broken down into specific units - start_time, hour, day, week, month, year, weekday



### Files in this project
    1) etl.py : reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
    2) sql_queries.py : contains all your sql queries, and is imported into the last two files above.
    3) README.md : provides discussion on your project.



### Tips to write this Project*
 1. Set Redshift cluster region as same as the S3 bucket(In this Project, Sparkify  data is in us-west-2 region)
 2. Load song_data files from S3 to staging takes lot of time. So I recomend you to restrict 'SONG_DATA' configuration as '/song_data/A/A/A/' to reduce load time.
