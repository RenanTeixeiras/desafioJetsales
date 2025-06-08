import requests
import os

def download_files():
    urls = {
        'municipios': 'https://dados.gov.br/ibge/municipios.csv',
        'populacao': 'https://dados.gov.br/ibge/estimativas_populacionais.csv',
        'cnes': 'https://dados.gov.br/cnes/estabelecimentos_saude.csv',
        'inep': 'https://dados.gov.br/inep/censo_escolar.csv',
    }
    os.makedirs('data/raw', exist_ok=True)
    for name, url in urls.items():
        response = requests.get(url)
        with open(f'data/raw/{name}.csv', 'wb') as f:
            f.write(response.content)