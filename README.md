# Sparkify Airflow Data Pipeline
Udacity Data Engineering Nanodegree Project 5: Data Pipelines

## Overview and Instruction

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Structure of the Project

dags/
* `udac_example_dag.py` is the core of the workflow which runs tasks and dependencies of the DAG. This should be placed in the `dags` directory.

queries/
* `create_tables.sql` is list of SQL for creating all required tables in Redshift. This should be placed in the `dags` directory.
* `sql_queries.py` is list of SQL queries that used in the ETL process. This should be placed in the `plugins/helpers` directory.

Custom Operators/
* `stage_redshift.py` defines `StageToRedshiftOperator` to copy data from S3 to staging tables in Redshift.
* `load_dimension.py` defines `LoadDimensionOperator` to load each dimension table(time, user, artist.. etc) from data in the staging tables.
* `load_fact.py` defines `LoadFactOperator` to load a fact table from data in the staging tables.
* `data_quality.py` defines `DataQualityOperator` to check data quality for consistency and accuracy. This checks row in target table contains 0 null values.

dependencies/
  	* start_operator >> create_tables
	* create_tables >> stage_events_to_redshift
	* create_tables >> stage_songs_to_redshift
	* stage_events_to_redshift >> load_songplays_table
	* stage_songs_to_redshift >> load_songplays_table
	* load_songplays_table >> load_user_dimension_table
	* load_songplays_table >> load_song_dimension_table
	* load_songplays_table >> load_artist_dimension_table
	* load_songplays_table >> load_time_dimension_table
	* load_user_dimension_table >> run_quality_checks
	* load_song_dimension_table >> run_quality_checks
	* load_artist_dimension_table >> run_quality_checks
	* load_time_dimension_table >> run_quality_checks
	* run_quality_checks >> end_operator


## Required Settings
* AWS Credentials
* Redshift Cluster
* Apache Airflow

    
    
## Additional Tips for run this projects
* run Airflow locally : https://github.com/brfulu/airflow-data-pipeline