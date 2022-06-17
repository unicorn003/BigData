from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator


with DAG(
       'lake_dag',
       default_args={
           'depends_on_past': False,
           'email': ['airflow@example.com'],
           'email_on_failure': False,
           'email_on_retry': False,
           'retries': 1,
           'retry_delay': timedelta(seconds=15),
       },
       description='A DAG for data lake',
       schedule_interval=None,
       start_date=datetime(2021, 1, 1),
       catchup=False,
       tags=['example'],
) as dag:
    dag.doc_md = """
       This is my lake DAG in airflow.
       I can write documentation in Markdown here with **bold text** or __bold text__.
   """

    def source_1():
        print(1)

    def source_2():
        print(2)

    def raw_1():
        print(3)

    def raw_2():
        print(4)

    def procedure_e():
        print(5)

    def index_elastic():
        print(6)


    s1 = PythonOperator(
        task_id ='sourceToRaw-1',
        python_callable = source_1,
    )

    s2 = PythonOperator(
        task_id ='sourceToRaw-2',
        python_callable = source_2,
    )

    r1 = PythonOperator(
        task_id='rawToFormatted-1',
        python_callable=raw_1,
    )

    r2 = PythonOperator(
        task_id ='rawToFormatted-2',
        python_callable = raw_2,
    )

    p = PythonOperator(
        task_id ='procedure-Usage',
        python_callable = procedure_e,
    )

    i = PythonOperator(
        task_id ='indexTo-Elastic',
        python_callable = index_elastic,
    )

    s1.set_downstream(r1)
    s2.set_downstream(r2)
    r1.set_downstream(p)
    r2.set_downstream(p)
    p.set_downstream(i)

