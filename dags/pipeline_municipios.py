from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append(f'{os.getcwd()}/')
from scripts.extract import *

default_args = {
    'owner': 'jetsales',
    'start_date': datetime(2025, 6, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='etl_ibge_censo_cras',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # Tarefas de extração
    extract_ibge_task = PythonOperator(
        task_id='extract_ibge_data',
        python_callable=download_ibge_data,
        provide_context=True,
    )
    
    extract_censo_task = PythonOperator(
        task_id='extract_censo_data',
        python_callable=extract_csv_to_dataframe1,
        provide_context=True,
    )
    
    extract_cras_task = PythonOperator(
        task_id='extract_cras_data',
        python_callable=extract_csv_to_dataframe2,
        provide_context=True,
    )
    
    extract_dtb_task = PythonOperator(
        task_id='extract_dtb_data',
        python_callable=extract_dtb_to_dataframe,
        provide_context=True,
    )
    
    # Tarefas de carregamento
    load_ibge_task = PythonOperator(
        task_id='load_ibge_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={'table_name': 'populacao', 'xcom_key': 'ibge_dataframe'},
        provide_context=True,
    )
    
    load_censo_task = PythonOperator(
        task_id='load_censo_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={'table_name': 'censo_escolar', 'xcom_key': 'censo_dataframe'},
        provide_context=True,
    )
    
    load_cras_task = PythonOperator(
        task_id='load_cras_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={'table_name': 'cras', 'xcom_key': 'cras_dataframe'},
        provide_context=True,
    )
    
    load_dtb_task = PythonOperator(
        task_id='load_dtb_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={'table_name': 'municipios', 'xcom_key': 'dtb_dataframe'},
        provide_context=True,
    )
    
    # Definir dependências
    extract_ibge_task >> load_ibge_task
    extract_censo_task >> load_censo_task
    extract_cras_task >> load_cras_task
    extract_dtb_task >> load_dtb_task