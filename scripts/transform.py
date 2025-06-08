import pandas as pd
import os

def process_files():
    os.makedirs('data/processed', exist_ok=True)
    
    # Exemplo: Padronizar tabela de municipios
    df_municipios = pd.read_csv('data/raw/municipios.csv', encoding='utf-8')
    df_municipios['cod_ibge'] = df_municipios['cod_ibge'].astype(str).str.zfill(7)
    df_municipios.to_csv('data/processed/municipios.csv', index=False, encoding='utf-8')

    # Similar para populacao, cnes, inep
    # Padronizar nomes de colunas, tipos de dados, remover duplicatas