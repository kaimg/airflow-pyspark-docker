from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.providers.http.operators.http import HttpOperator # type: ignore
from airflow.sdk import Variable

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def check_response(response):
    return response.status_code == 200

def show_response(ti):
    response = ti.xcom_pull(task_ids="get_api_response")
    API_KEY = Variable.get("API_KEY_CURRENCY_RATE")
    print(f"API KEY: {API_KEY}")
    print(f"Our response: {response}")

with DAG(
    dag_id="currency_new_dag_python_operator",
    default_args=default_args,
    description="Run Currency ETL job using PythonOperator",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["currency"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    API_KEY = Variable.get("API_KEY_CURRENCY_RATE")
    get_api_response = HttpOperator(
        task_id="get_api_response",
        http_conn_id="http_currency",
        endpoint=f"/latest?access_key={API_KEY}&base=EUR",
        method="GET",
        response_check=check_response,
        
    )
    print_response = PythonOperator(
        task_id="print_response",
        python_callable=show_response,
    )

    (start >> get_api_response >> print_response >> end)
