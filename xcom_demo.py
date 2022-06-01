from airflow import DAG
from airflow.operators.python import PythonOperator

import datetime


default_args = {
    'start_date': datetime.datetime(2022, 1, 1)
}


def _push_func(**kwargs):
    message = 'This is from push function.'
    ti = kwargs['ti']
    ti.xcom_push(key='message', value=message)


def _pull_func(**kwargs):
    ti = kwargs['ti']
    push_mess = ti.xcom_pull(key='message')
    print("PULL message: '%s'" % push_mess)


with DAG(dag_id='xcom_demo', default_args=default_args, schedule_interval='@daily', catchup=False,) as dags:
    t1 = PythonOperator(
        task_id='t1',
        python_callable=_push_func,
        provide_context=True
    )

    t2 = PythonOperator(
        task_id='t2',
        python_callable=_pull_func,
        provide_context=True
    )

    t1 >> t2
