import traceback
from datetime import datetime
from airflow.decorators import dag, task

import base_methods as funs
import base_query as query

DB_CONN_ID = 'data_source_connection_name'
AWS_S3_CONN_ID ='aws connection id'

@dag(
    dag_id='base_dag',
    schedule_interval='40 2 * * *'
    ,   start_date=datetime(2025, 2, 1)
    ,   catchup=False
)
def base_dag():
    @task
    def extract_data():
        print("extract_data")
        return "extrack data result"

    @task
    def transform_data():
        print("transform_data")
        return "transform_data result"

    @task
    def load_data():
        print("load_data")
        return "load_data"

    @task
    def when_exist_args_task(args):
        print(f"args -> {args}")

    @task
    def when_database_connection_task():
        conn, cursor = funs.init(DB_CONN_ID)
        try:
            cursor.execute(query.SAMPLE_SELECT_QUERY)
        except Exception as e:
            conn.rollback()
            print("".join(traceback.format_exc()))
        finally:
            conn.commit()

    extract_data()
    transform_data()
    result = load_data()

    when_exist_args_task(result)
    when_database_connection_task()

dag_run = base_dag()