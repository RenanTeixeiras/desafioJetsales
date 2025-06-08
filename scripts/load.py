import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def create_tables():
    conn = psycopg2.connect("dbname=airflow user=airflow password=airflow host=postgres")
    with open('sql/schema.sql') as f:
        conn.cursor().execute(f.read())
    conn.commit()
    conn.close()

def load_to_postgres():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    for table in ['municipios', 'populacao', 'saude_cnes', 'educacao_inep']:
        df = pd.read_csv(f'data/processed/{table}.csv')
        df.to_sql(table, engine, if_exists='append', index=False)