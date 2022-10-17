from datetime import datetime, timedelta
import os

from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.subdag import SubDagOperator
from subdag import create_tables_subdag


from plugins.stage_redshift import StageToRedshiftOperator
from plugins.load_fact import LoadFactOperator
from plugins.load_dimension import LoadDimensionOperator
from plugins.data_quality import DataQualityOperator


from plugins.sql_queries import SqlQueries


#tables = ["staging_events", "staging_songs", "songplays", "users", "songs", "artists", "time"]

############################################ DAG ############################################


default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2022, 8, 11, 3, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
    }


with DAG(
    dag_id='airflow_sparkify',
    description='Load and transform data in Redshift with Airflow',
    default_args=default_args,
    schedule_interval='@hourly',
    max_active_runs=1
    ) as dag:


#########################################################################################
######################################### TASKS #########################################
#########################################################################################

    start_operator_task = DummyOperator(
        task_id='Begin_execution'
    )

    end_operator_task = DummyOperator(
        task_id='Stop_execution'
    )


################################### CREATE TABLES SUBDAG ###################################

    create_table_group_tasks = SubDagOperator(
        task_id = 'Create_all_tables',
        subdag=create_tables_subdag('airflow_sparkify', 'Create_all_tables', default_args)
    )


######################################### STAGING #########################################

    stage_events_to_redshift_task = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='public.staging_events',
        s3_bucket='udacity-dend',
        s3_key="log_data",
        region='us-west-2',
        extra_params='log_json_path.json'

    )

    stage_songs_to_redshift_task = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='public.staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        extra_params='auto'
    )


######################################### 1-Check #########################################
################################ Comparison Not Recognized #################################

#    checks = [
#        {'test_sql': "select count(*) from staging_events;", 'expected_result': 0, comparison: '>'},
#        {'test_sql': "select count(*) from staging_songs;", 'expected_result': 0, comparison: '>'}
#    ]

#    first_run_quality_checks = DataQualityOperator(
#        task_id='First_run_data_quality_checks',
#        redshift_conn_id='redshift',
#        dq_checks=checks
#    )
#    checks = []

######################################### FACT TABLE #########################################

    load_songplays_table_task = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql_stmt=SqlQueries.songplay_table_insert,
        append_only=True
    )


######################################### DIMENSIONS TABLE #########################################

    load_users_dimension_table_task = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql_stmt=SqlQueries.user_table_insert,
        append_only=False
    )

    load_songs_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql_stmt=SqlQueries.song_table_insert,
        append_only=False
    )

    load_artists_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql_stmt=SqlQueries.artist_table_insert,
        append_only=False
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql_stmt=SqlQueries.time_table_insert,
        append_only=False
    )
######################################### 2-Check #########################################

    checks = [
        {'test_sql': "select count(*) from artists where artist_id isnull;", 'expected_result': 0},
        {'test_sql': "select count(*) from songs where song_id isnull;", 'expected_result': 0},
        {'test_sql': "select count(*) from users where userid isnull;", 'expected_result': 0},
        {'test_sql': "select count(*) from time where start_time isnull;", 'expected_result': 0},
        {'test_sql': "select count(*) from songplays where playid isnull;", 'expected_result': 0}
    ]

# for: {'test_sql': "select count(*) from songs where song_id isnull;", 'expected_result': 1}
# Result: Failed to execute job 447 for task Dimensions_run_quality_checks 
#         (Data quality check #1 failed. The query: << select count(*) from songs where song_id isnull; >> fail to pass; 13975)


    Dimensions_run_quality_checks = DataQualityOperator(
        task_id='Dimensions_run_quality_checks',
        redshift_conn_id='redshift',
        dq_checks=checks
    )


######################################### TASK INSTANCES #########################################

    start_operator_task >>  create_table_group_tasks >> [stage_events_to_redshift_task, stage_songs_to_redshift_task] >> load_songplays_table_task 
    load_songplays_table_task >> [load_users_dimension_table_task, load_songs_dimension_table, 
                                                                load_artists_dimension_table, load_time_dimension_table]  >> Dimensions_run_quality_checks
    Dimensions_run_quality_checks >> end_operator_task