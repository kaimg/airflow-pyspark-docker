from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.empty import EmptyOperator  # type: ignore
from airflow.providers.http.operators.http import HttpOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
    ENDPOINT_URL = f"/latest?access_key={API_KEY}&base=EUR&symbols=USD,AUD,CAD,PLN,MXN,AZN,TRY"
    get_api_response = HttpOperator(
        task_id="get_api_response",
        http_conn_id="http_currency",
        endpoint= ENDPOINT_URL,
        method="GET",
        response_check=check_response,
        
    )
    print_response = PythonOperator(
        task_id="print_response",
        python_callable=show_response,
    )
    write_response = SQLExecuteQueryOperator(
        task_id='write_response',
        conn_id='my_postgres_conn',
        sql="""
            {% for rate_currency, rate in ti.xcom_pull(task_ids='process_data', key='currency_data')['rates'].items() %}
              INSERT INTO currency_rates (base_currency, rate_currency, rate, timestamp, date)
              VALUES ('{{ ti.xcom_pull(task_ids='process_data', key='currency_data')['base'] }}', '{{ rate_currency }}', {{ rate }}, '{{ ti.xcom_pull(task_ids='process_data', key='currency_data')['timestamp'] }}', '{{ ti.xcom_pull(task_ids='process_data', key='currency_data')['date'] }}');
            {% endfor %}
        """, 
    )

    (start >> get_api_response >> print_response >> write_response >> end)
