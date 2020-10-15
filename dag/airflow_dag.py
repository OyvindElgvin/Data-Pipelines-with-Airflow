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

# start airflow with this:
# /opt/airflow/start.sh

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')



default_args = {
    'owner': 'udacity',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}



dag = DAG('udac_example_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '0 * * * *',
          max_active_runs = 1,
        )

# this just starts the dag
start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

# operator that gets sql queries from create_tables.sql
# create all public tables needed
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
    s3_key = "song_data/",   #   test sample: song_data/A/A/A/      whole data set: song_data/
    json = 'auto'
)

# operator plugin that import sql query and generate a fact table
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    table = 'songplays',
    redshift_conn_id = "redshift",
    sql_stm = SqlQueries.songplay_table_insert,
)

# operator plugin that creates users table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table = 'users',
    redshift_conn_id = "redshift",
    sql_stm = SqlQueries.user_table_insert,
    append_data = True,
)

# operator plugin that creates songs table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table = 'songs',
    redshift_conn_id = "redshift",
    sql_stm = SqlQueries.song_table_insert,
    append_data = True,
)

# operator plugin that creates artists table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table = 'artists',
    redshift_conn_id = "redshift",
    sql_stm = SqlQueries.artist_table_insert,
    append_data = True,
)

# operator plugin that creates time table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table = 'time',
    redshift_conn_id = "redshift",
    sql_stm = SqlQueries.time_table_insert,
    append_data = True,
)

# dictionary with test querys and results
test_and_result = {"""SELECT COUNT({}) FROM {} WHERE {} IS NULL""": 0,
                   "SELECT COUNT({}) FROM {} WHERE {} IS NOT NULL": 0}

# set the comparison operations for the corresponding test query in 'test_and_result'
# available comparison operations expressions for automation are '==', '<', '>'. See data_quality.py for clearification
comparison_operation_list = ['==','>']

# dict with tables and columns
# could also check more columns in each table
table_col_dict = {'users': 'userid',
                   'songs': 'songid',
                   'artists': 'artistid',
                   'time': 'start_time'}

# operator plugin that runs a data quality check on the loaded data
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id = "redshift",
    table_col_dict = table_col_dict,
    test_and_result = test_and_result,
    comparison_operation_list = comparison_operation_list,
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
