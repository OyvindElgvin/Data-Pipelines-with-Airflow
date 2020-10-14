from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator,
                               LoadFactOperator,
                               LoadDimensionOperator,
                               DataQualityOperator)

from helpers import SqlQueries

# /opt/airflow/start.sh

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

# 'start_date': datetime(2019, 1, 12)



default_args = {
    'owner': 'udacity',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'retries': 0, #3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}



dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = None, #'@hourly',     #'0 * * * *'
          max_active_runs = 1 # denne kan vel sloyfes?
        )

# this just starts the dag
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

# operator that gets sql queries from create_tables.sql
# and create all public tables needed
create_tables_task = PostgresOperator(
  task_id="create_tables",
  dag=dag,
  sql=['create_tables.sql'],
  postgres_conn_id="redshift"
)

# operator plugin creating a staging table form the log file
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    dag = dag,
    table = "staging_events",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    json = 's3://udacity-dend/log_json_path.json'
)

# operator plugin creating a staging table form the song file
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table = "staging_songs",
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    s3_bucket = "udacity-dend",
    s3_key = "song_data/A/A/A/",   #   A/A/A/   */*/*/
    json = 'auto'
)

# operator plugin that import sql query and generate a fact table
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = 'songplays',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    sql_stm = SqlQueries.songplay_table_insert,
)

# operator plugin that creates users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    sql_stm = SqlQueries.user_table_insert,
    append_data = True,
)

# operator plugin that creates songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    sql_stm = SqlQueries.song_table_insert,
    append_data = True,
)

# operator plugin that creates artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artists',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    sql_stm = SqlQueries.artist_table_insert,
    append_data = True,
)

# operator plugin that creates time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'time',
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    sql_stm = SqlQueries.time_table_insert,
    append_data = True,
)

# operator plugin that runs a data quality check on the loaded data
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    table_list = ['users', 'artists'],
    columns = ['userid', 'artistid'],
    redshift_conn_id = "redshift",
    sql_stm = f"""SELECT sum(case when {{}} is null then 1 else 0 end) as {{}} FROM {{}}""",
)

# operator that stops the dag
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# two lists with parrallell tasks
stage_list = [stage_events_to_redshift, stage_songs_to_redshift]

load_dimension_tables_list = [load_user_dimension_table,
                             load_song_dimension_table,
                             load_artist_dimension_table,
                             load_time_dimension_table]

# order of tasks
start_operator >> create_tables_task >> stage_list
stage_list >> load_songplays_table >> load_dimension_tables_list
load_dimension_tables_list >> run_quality_checks >> end_operator
