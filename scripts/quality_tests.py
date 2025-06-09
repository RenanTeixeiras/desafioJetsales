import psycopg2

def run_tests():
    conn = psycopg2.connect("dbname=airflow user=airflow password=airflow host=postgres")
    cur = conn.cursor()

    # Teste 1: Verificar códigos IBGE nas tabelas 
    cur.execute("SELECT chave_ibge FROM populacao WHERE chave_ibge NOT IN (SELECT chave_ibge FROM municipios)")
    if cur.fetchall():
        raise ValueError("Códigos IBGE inválidos encontrados na tabela populacao")
    
    cur.execute("SELECT chave_ibge FROM cras WHERE chave_ibge NOT IN (SELECT chave_ibge FROM municipios)")
    if cur.fetchall():
        raise ValueError("Códigos IBGE inválidos encontrados na tabela cras")
    
    cur.execute("SELECT chave_ibge FROM cras WHERE chave_ibge NOT IN (SELECT chave_ibge FROM municipios)")
    if cur.fetchall():
        raise ValueError("Códigos IBGE faltantes na tabela cras")
    
    cur.execute("SELECT chave_ibge FROM populacao WHERE chave_ibge NOT IN (SELECT chave_ibge FROM municipios)")
    if cur.fetchall():
        raise ValueError("Códigos IBGE faltantes na tabela populacao")

    # Teste 2: Verificar duplicatas
    cur.execute("SELECT chave_ibge, COUNT(*) FROM populacao GROUP BY chave_ibge HAVING COUNT(*) > 1;")
    if cur.fetchall():
        raise ValueError("Duplicatas encontradas na tabela populacao")
    
    cur.execute("SELECT chave_ibge, COUNT(*) FROM cras GROUP BY chave_ibge HAVING COUNT(*) > 1;")
    if cur.fetchall():
        raise ValueError("Duplicatas encontradas na tabela cras")
    
    cur.execute("SELECT chave_ibge, COUNT(*) FROM municipios GROUP BY chave_ibge HAVING COUNT(*) > 1;")
    if cur.fetchall():
        raise ValueError("Duplicatas encontradas na tabela municipios")


    # Teste 3: Valores fora do esperado
    cur.execute('SELECT * FROM populacao WHERE "POPULACAO_ESTIMADA" < 0')
    if cur.fetchall():
        raise ValueError("População negativa encontrada")
    
    cur.execute('''
            SELECT *
            FROM populacao
            WHERE "POPULACAO_ESTIMADA" IS NULL OR "POPULACAO_ESTIMADA" = 0;
            ''')
    if cur.fetchall():
        raise ValueError("População estimada com valor nulo ou zero")
    
    cur.execute('''
            SELECT *
            FROM cras
            WHERE nome IS NULL OR endereco IS NULL OR bairro IS NULL OR cidade IS NULL;
            ''')
    if cur.fetchall():
        raise ValueError("Coluna CRAS com campos obrigatórios faltantes")


    conn.close()