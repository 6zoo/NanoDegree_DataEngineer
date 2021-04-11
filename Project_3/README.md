# Udacity NanoDegree - Data Engineer
## Project.3 Cloud Data Ware House

--

### Introduction
 As a Data Engineer of "Sparkify" which is a start-up and operates Music streaming Services, I built Cloud DW in AWS Redshift and ETL pipelines using with Python from S3 bucket to staging tables of Song Data and Log Data(User Behavioral logs).
And In Redshift, transformed data in to set of dimensional table and fact table for Analytics teams to perform analysis with their perspectives.


### How to run this project
    1. Open Terminal
    2. Set your configuration information, and save it as dwh.cfg in the root folder
       1) You should create your Redshift Cluster and IAM role
       2) Redshift IAM role should contain 'AmazonS3ReadOnlyAccess' policy.
    3. Run create_tables.py (!python create_tables.py) - It sets up Database and Tables.
    4. Run etl.py (!python etl.py) - It processes and loads data into tables created by 2 procedure.
    
    - Option) If you want to verify the results, Run test queries in test.ipynb

### DataSets
     1. Song data
     2. Log data 

### Database Schema
 - Database schema of this project was built in "Snow Flake" modeling.
 - Consists of 1 Fact Table, and 4 Dimension Tables

#### 1) Staging Tables
 - staging_events
 - staging_songs

#### 2) Fact Table
 - songplays - records in event data associated with song plays i.e. records with page NextSong - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

#### 3) Dimension Tables
 - users - users in the app - user_id, first_name, last_name, gender, level
 - songs - songs in music database - song_id, title, artist_id, year, duration
 - artists - artists in music database - artist_id, name, location, lattitude, longitude
 - time - timestamps of records in songplays broken down into specific units - start_time, hour, day, week, month, year, weekday



### Files in this project
    1) create_tables.py : drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
    2) etl.py : reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.
    3) sql_queries.py : contains all your sql queries, and is imported into the last two files above.
    4) README.md : provides discussion on your project.



### Tips to write this Project*
 1. set VPC of Redshift Cluster to public accesible for access from the jupyter notebook.
 2. Set HOST variable in dwh.cfg file as Endpoint from Redshift cluster dashboard
 3. Set Redshift cluster region as same as the S3 bucket(In this Project, Sparkify  data is in us-west-2 region)
 4. IAM role of the Redshift cluster should have S3 Access available to get copy data from the S3 bucket by proxy
 5. You can create redshift cluster and iam role from both AWS Redshift and Codes
 6. Load song_data files from S3 to staging takes lot of time. So I recomend you to restrict 'SONG_DATA' configuration as '/song_data/A/A/A/' to reduce load time.
