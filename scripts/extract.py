import zipfile
import requests
import pandas as pd
from io import BytesIO, StringIO
from sqlalchemy import create_engine


def download_ibge_data(**kwargs):
    # URL da planilha
    url = "https://ftp.ibge.gov.br/Estimativas_de_Populacao/Estimativas_2024/POP2024_20241230.xls"
    
    # Fazer o download do arquivo
    response = requests.get(url)
    response.raise_for_status()  # Levanta um erro se o download falhar
    
    # Ler a planilha diretamente em um DataFrame
    df = pd.read_excel(BytesIO(response.content), sheet_name=1)
    
    # Remover a primeira linha
    df = df.iloc[1:].reset_index(drop=True)

    df.rename(columns={
        'ESTIMATIVAS DA POPULAÇÃO RESIDENTE NOS MUNICÍPIOS BRASILEIROS COM DATA DE REFERÊNCIA EM 1º DE JULHO DE 2024':'UF',
        'Unnamed: 1':'COD. UF', 
        'Unnamed: 2':'COD_MUNIC', 
        'Unnamed: 3':'NOME', 
        'Unnamed: 4':'POPULACAO_ESTIMADA', 
        }, inplace=True)
    print(df.columns)

    # Salvar no XCom
    kwargs['ti'].xcom_push(key='ibge_dataframe', value=df.to_dict())
    
    # Opcional: Exibir as primeiras linhas para verificação
    print(df.head())
    
    return df

def extract_csv_to_dataframe1(**kwargs):
    # URL do arquivo ZIP
    url = "https://download.inep.gov.br/dados_abertos/microdados_censo_escolar_2024.zip"
    
    # Fazer o download do arquivo ZIP
    response = requests.get(url)
    response.raise_for_status()
    
    # Abrir o arquivo ZIP em memória
    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        csv_path = "microdados_censo_escolar_2024/dados/microdados_ed_basica_2024.csv"
        if csv_path in zip_ref.namelist():
            with zip_ref.open(csv_path) as csv_file:
                df = pd.read_csv(csv_file, encoding='latin1', sep=';')
        else:
            raise FileNotFoundError(f"Arquivo {csv_path} não encontrado no ZIP")
    
    # Salvar no XCom
    kwargs['ti'].xcom_push(key='censo_dataframe', value=df.to_dict())
    
    return df

def extract_csv_to_dataframe2(**kwargs):
    # URL do arquivo CSV
    url = "https://aplicacoes.mds.gov.br/sagi/servicos/equipamentos?q=tipo_equipamento:CRAS&wt=csv&fl=id_equipamento,ibge,uf,cidade,nome,responsavel,telefone,endereco,numero,complemento,referencia,bairro,cep,georef_location,data_atualizacao&rows=999999999"
    
    # Fazer o download do arquivo CSV
    response = requests.get(url)
    response.raise_for_status()
    text = response.text
    
    # Ler o CSV diretamente em um DataFrame
    df = pd.read_csv(
        StringIO(text),
        encoding='utf-8',
        sep=',',
        quotechar='"',
        engine='python',
        on_bad_lines='skip'
    )
    
    # Salvar no XCom
    kwargs['ti'].xcom_push(key='cras_dataframe', value=df.to_dict())
    
    return df

def extract_dtb_to_dataframe(**kwargs):
    # URL do arquivo ZIP
    url = "https://geoftp.ibge.gov.br/organizacao_do_territorio/estrutura_territorial/divisao_territorial/2024/DTB_2024.zip"
    
    # Fazer o download do arquivo ZIP
    response = requests.get(url)
    response.raise_for_status()
    
    # Abrir o arquivo ZIP em memória
    with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
        excel_path = "RELATORIO_DTB_BRASIL_2024_MUNICIPIOS.xls"
        if excel_path in zip_ref.namelist():
            with zip_ref.open(excel_path) as excel_file:
                df = pd.read_excel(excel_file, skiprows=6)
        else:
            raise FileNotFoundError(f"Arquivo {excel_path} não encontrado no ZIP")
    
    # Salvar no XCom
    kwargs['ti'].xcom_push(key='dtb_dataframe', value=df.to_dict())
    
    # Opcional: Exibir as primeiras linhas para verificação
    print(df.head())
    
    return df

def load_to_postgres(**kwargs):
    # Criar o engine do PostgreSQL
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    # Recuperar o DataFrame do XCom
    task_instance = kwargs['ti']
    table_name = kwargs['table_name']
    xcom_key = kwargs['xcom_key']
    df_dict = task_instance.xcom_pull(key=xcom_key)
    df = pd.DataFrame.from_dict(df_dict)
    
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            schema='public',
            if_exists='replace',
            index=False
        )
        print(f"Dados carregados com sucesso na tabela {table_name}")
    except Exception as e:
        print(f"Erro ao carregar dados no PostgreSQL: {e}")
        raise

def execute():
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    
    load_to_postgres(extract_dtb_to_dataframe(), 'municipios', engine)
    load_to_postgres(extract_csv_to_dataframe1(), 'censo_escolar', engine)
    load_to_postgres(extract_csv_to_dataframe2(), 'CRAS', engine)
    load_to_postgres(download_ibge_data(), 'populacao', engine)


