import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import boto3

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator


def create_s3_bucket():
    client = boto3.client(
        "s3",
        aws_access_key_id='AKIA32NEX5DYHYH6CQDB',
        aws_secret_access_key='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',
    )
    client.create_bucket(Bucket='From-Postgres')


def _upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
    dag_id="postgres_to_s3",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    # [START postgres_operator_howto_guide_create_pet_table]
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS pets (
            pets_id SERIAL PRIMARY KEY NOT NULL,
            name VARCHAR NOT NULL,
            pets_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
    )
    # [END postgres_operator_howto_guide_create_pet_table]
    # [START postgres_operator_howto_guide_populate_pet_table]
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id='postgres',
        sql="""
            INSERT INTO pets (name, pets_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pets (name, pets_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pets (name, pets_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pets (name, pets_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    # [END postgres_operator_howto_guide_populate_pet_table]
    # [START postgres_operator_howto_guide_get_all_pets]
    get_all_pets = PostgresOperator(
        task_id="get_all_pets",
        postgres_conn_id='postgres',
        sql="SELECT * FROM pets;")
    # [END postgres_operator_howto_guide_get_all_pets]
    # [START postgres_operator_howto_guide_get_birth_date]
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id='postgres',
        sql="SELECT * FROM pets WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        runtime_parameters={'statement_timeout': '3000ms'},
    )
    to_create_csv = BashOperator(
        task_id='to_create_csv',
        bash_command="touch /home/shubham/Desktop/pet2.csv && chmod 777 /home/shubham/Desktop/pet2.csv"
    )

    create_aws_bucket = PythonOperator(
        task_id='create_aws_bucket',
        python_callable=create_s3_bucket,
    )

    to_csv = PostgresOperator(
        task_id='to_csv',
        postgres_conn_id='postgres',
        #sql="COPY pets TO '/home/shubham/Desktop/pet2.csv' DELIMITER ',' CSV HEADER;"
        sql="COPY pets TO '/home/shubham/Desktop/pet2.csv' DELIMITER ',' CSV HEADER;"
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=_upload_to_s3,
        op_kwargs={
            'filename': '/home/shubham/Desktop/pet2.csv',
            'key': 'pet2.csv',
            'bucket_name': 'postgres12postgres'
        }
    )
    # [END postgres_operator_howto_guide_get_birth_date]
    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date >> to_create_csv >> create_aws_bucket >> to_csv >> upload_to_s3
    # [END postgres_operator_howto_guide]
