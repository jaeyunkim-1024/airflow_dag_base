import traceback
from datetime import datetime

from airflow.decorators import dag, task
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models.variable import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def download_from_s3(aws_conn_id,local_csv_path,file_name):
    key = f"bucket key path/{file_name}"
    bucket_name = Variable.get("bucket name")

    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    isExist = s3_hook.check_for_key(bucket_name=bucket_name,key=key)

    if not isExist:
        return False
    try:
        # download_file 시, airflow_tmp_n1d3xni0 로 저장되면서 csv가 안됨. 왜일까
        data = s3_hook.read_key(
            key=key,
            bucket_name=bucket_name
        )
        with open(local_csv_path, "w", encoding="utf-8") as f:
            f.write(data)

        return True
    except Exception as e:
        print(f"Error When download, {e}")
        return False

def connect(mysql_conn_id):
    source_hook = MySqlHook(mysql_conn_id=mysql_conn_id)
    conn = source_hook.get_conn()
    return conn

def init(mysql_conn_id):
    conn = connect(mysql_conn_id)
    cursor = conn.cursor()
    return conn,cursor