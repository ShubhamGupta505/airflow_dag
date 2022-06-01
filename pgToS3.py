from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pyarrow.parquet as pq
import boto3
import pandas as pd
import os.path
import datetime
import psycopg2 as pg

defalut_args = {
    'start_date': datetime.datetime(2022, 1, 1)
}


def _file_available_or_not():
    if os.path.exists('/home/shubham/Desktop/BankChurners.csv') == True:
        print("File is Available")
    else:
        print("File doesn't Exists!")
        print("OR")
        print("Check File Path is correct or not!")


def print_data_from_csv():
    df = pd.read_csv('/home/shubham/Desktop/BankChurners.csv')
    print(df.head())


def column_data_type():
    df = pd.read_csv('/home/shubham/Desktop/BankChurners.csv')
    df.info()


def is_null_value():
    df = pd.read_csv('/home/shubham/Desktop/BankChurners.csv')
    print(df.isnull().any())


def _print_data():
    engine = pg.connect(
        "dbname='postgres' user='postgres' host='127.0.0.1' port='5432' password='5200'")
    df = pd.read_sql('select * from BankChurners limit 5', con=engine)
    df = df.dropna()
    df = df.to_parquet(
        '/home/shubham/Desktop/BankChurners.parquet', index=False)


def create_s3_bucket():
    client = boto3.client(
        "s3",
        aws_access_key_id='AKIA32NEX5DYHYH6CQDB',
        aws_secret_access_key='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',
    )
    client.create_bucket(Bucket='from-postgres-to-aws')


def _upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(dag_id="pgTos3", default_args=defalut_args, schedule_interval='@daily', catchup=False,) as dags:

    is_file_available = PythonOperator(
        task_id='is_file_available',
        python_callable=_file_available_or_not,
    )

    print_csv_data = PythonOperator(
        task_id='print_csv_data',
        python_callable=print_data_from_csv,
    )

    check_null_value = PythonOperator(
        task_id='check_null_value',
        python_callable=is_null_value,
    )

    print_column_data_type = PythonOperator(
        task_id='print_column_data_type',
        python_callable=column_data_type,
    )

    create_bank_table = PostgresOperator(
        task_id='create_bank_table',
        postgres_conn_id='postgres',
        sql="""
        CREATE TABLE IF NOT EXISTS BankChurners(
            CLIENTNUM INT,
            Attrition_Flag VARCHAR(30),
            Customer_Age INT,
            Gender VARCHAR(5),
            Dependent_count INT,
            Education_Level VARCHAR(20),
            Marital_Status VARCHAR(20),
            Income_Category VARCHAR(20),
            Card_Category VARCHAR(10),
            Months_on_book INT,
            Total_Relationship_Count INT,
            Months_Inactive_12_mon INT,
            Contacts_Count_12_mon INT,
            Credit_Limit FLOAT,
            Total_Revolving_Bal INT,
            Avg_Open_To_Buy FLOAT,
            Total_Amt_Chng_Q4_Q1 FLOAT,
            Total_Trans_Amt INT,
            Total_Trans_Ct INT,
            Total_Ct_Chng_Q4_Q1 FLOAT,
            Avg_Utilization_Ratio FLOAT
        );""",
    )

    insert_bank_data = PostgresOperator(
        task_id='insert_bank_data',
        postgres_conn_id='postgres',
        sql="""
        COPY BankChurners FROM '/home/shubham/Desktop/BankChurners.csv' DELIMITER ',' HEADER CSV
        """
    )

    grant_permission = PostgresOperator(
        task_id='grant_permission',
        postgres_conn_id='postgres',
        sql="""
        GRANT ALL ON TABLE public.BankChurners To pg_read_all_data;
        """
    )

    print_bank_data = PythonOperator(
        task_id='print_bank_data',
        python_callable=_print_data,
    )

    create_aws_bucket = PythonOperator(
        task_id='create_aws_bucket',
        python_callable=create_s3_bucket,
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=_upload_to_s3,
        op_kwargs={
            'filename': '/home/shubham/Desktop/BankChurners.parquet',
            'key': 'BankChurners.parquet',
            'bucket_name': 'from-postgres-to-aws'
        }
    )

    is_file_available >> print_csv_data >> check_null_value >> print_column_data_type >> create_bank_table >> insert_bank_data >> grant_permission >> print_bank_data >> create_aws_bucket >> upload_to_s3
