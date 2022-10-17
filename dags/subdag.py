from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sql_create_tables


def create_tables_subdag(parent_dag_id, subdag_dag_id, default_args):
    with DAG(f"{parent_dag_id}.{subdag_dag_id}", default_args=default_args) as dag:

        # create staging_events table
        create_staging_events_table_task = PostgresOperator(
            task_id='create_staging_events_table',
            postgres_conn_id='redshift',
            sql=sql_create_tables.create_staging_events_table_sql
        )

        # create_staging_songs_table_sql
        create_staging_songs_table_task = PostgresOperator(
            task_id='create_staging_songs_table',
            postgres_conn_id='redshift',
            sql=sql_create_tables.create_staging_songs_table_sql
        )

        # create artists table
        create_artists_table_task = PostgresOperator(
            task_id='create_artists_table',
            postgres_conn_id='redshift',
            sql=sql_create_tables.create_artists_table_sql
        )

        # create songs table
        create_songs_table_task = PostgresOperator(
            task_id='create_songs_table',
            postgres_conn_id='redshift',
            sql=sql_create_tables.create_songs_table_sql
        )

        # create users table
        create_staging_events_table_task = PostgresOperator(
            task_id='create_users_table',
            postgres_conn_id='redshift',
            sql=sql_create_tables.create_users_table_sql
        )

        # create time table
        create_time_table_task = PostgresOperator(
            task_id='create_time_table',
            postgres_conn_id='redshift',
            sql=sql_create_tables.create_time_table_sql
        )


        return dag