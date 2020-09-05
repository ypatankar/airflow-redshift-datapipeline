from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator

from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator
)


from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# default arguments for dag
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1, 0, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False
}

# Dag to load, tranform and quality control data in Redshift with Airflow
dag = DAG('sparkify_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# task to create tables for Sparkify
create_table = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)

# Task to stage events data in Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data/{execution_date.year}/{execution_date.month}",
    json_path="log_json_path.json"
)

# Task to stage songs data in Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song-data/A/",
    json_path=""
)

# Task to load songplays fact in Redshift
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    mode="append"
)

# Task to load user dimension in Redshift
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    mode="truncate-insert"
)

# Task to load song dimension in Redshift
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    mode="truncate-insert"
)

# Task to load artist dimension in Redshift
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    mode="truncate-insert"
)

# Task to load time dimension in Redshift
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="times",
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    mode="truncate-insert"
)

# Task to perform quality checks on the data uploaded in Redshift
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    qa_check_list = [
{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM users", 'expected_result': 104},
{'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid is null AND userid IS NULL", 'expected_result': 0},
{'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}
],
    redshift_conn_id="redshift"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# order of execution for the dag
start_operator >> create_table
create_table >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
