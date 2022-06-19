from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.operators.bash import BashOperator

from lib.imdb_fetcher import fetch_data_from_imdb
from lib.raw_to_fmt_imdb import convert_raw_to_formatted_imdb
from lib.data_fetcher import fetch_data_from_kaagle
from lib.combine_data import combine_data_imdb
from lib.raw_to_fmt_kaagle import convert_raw_to_formatted_kaagle

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

    s1 = PythonOperator(
        task_id ='sourceToRaw-1',
        python_callable = fetch_data_from_kaagle(url='mpwolke/cusersmarildownloadscinemacsv', data_entity_name="MoviePercent", file_name="cinema.csv"),
    )

    s2 = PythonOperator(
        task_id ='sourceToRaw-2',
        python_callable = fetch_data_from_imdb(url='https://datasets.imdbws.com/title.ratings.tsv.gz', data_entity_name = 'MovieRating', file_name ='title.ratings.tsv.gz'),
    )

    r1 = PythonOperator(
        task_id='rawToFormatted-1',
        python_callable=convert_raw_to_formatted_kaagle(file_name='cinema.csv', current_day=date.today().strftime("%Y%m%d")),
    )

    r2 = PythonOperator(
        task_id ='rawToFormatted-2',
        python_callable = convert_raw_to_formatted_imdb(file_name = "title.ratings.tsv.gz", current_day=date.today().strftime("%Y%m%d")),
    )

    p = PythonOperator(
        task_id ='procedure-Usage',
        python_callable = combine_data_imdb(current_day=date.today().strftime("%Y%m%d")),
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

