from airflow import DAG
from airflow.operators import PythonOperator
from datetime import datetime
import scripts.extract as extract
import scripts.transform as transform
import scripts.load as load
import scripts.quality_tests as quality_tests

default_args = {
    'owner': 'jetsales',
    'start_date': datetime(2025, 6, 7),
    'retries': 1,
}

with DAG(
    'pipeline_municipios',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract.download_files,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform.process_files,
    )

    create_tables = PythonOperator(
        task_id='create_tables',
        python_callable=load.create_tables,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load.load_to_postgres,
    )

    quality_check = PythonOperator(
        task_id='quality_check',
        python_callable=quality_tests.run_tests,
    )

    extract_data >> transform_data >> create_tables >> load_data >> quality_check