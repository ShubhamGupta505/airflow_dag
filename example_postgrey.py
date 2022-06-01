import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash import BashOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2022, 1, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:
    # [START postgres_operator_howto_guide_create_pet_table]
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id='postgres',
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
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
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
    )
    # [END postgres_operator_howto_guide_populate_pet_table]
    # [START postgres_operator_howto_guide_get_all_pets]
    get_all_pets = PostgresOperator(
        task_id="get_all_pets",
        postgres_conn_id='postgres',
        sql="SELECT * FROM pet;")
    # [END postgres_operator_howto_guide_get_all_pets]
    # [START postgres_operator_howto_guide_get_birth_date]
    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id='postgres',
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
        runtime_parameters={'statement_timeout': '3000ms'},
    )
    to_create_csv = BashOperator(
        task_id='to_create_csv',
        bash_command="touch /home/shubham/Desktop/pets.csv && chmod 777 /home/shubham/Desktop/pets.csv"
    )

    to_csv = PostgresOperator(
        task_id='to_csv',
        postgres_conn_id='postgres',
        sql="COPY pet TO '/home/shubham/Desktop/pets.csv' DELIMITER ',' CSV HEADER;"
    )
    # [END postgres_operator_howto_guide_get_birth_date]
    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date >> to_create_csv >> to_csv
    # [END postgres_operator_howto_guide]
