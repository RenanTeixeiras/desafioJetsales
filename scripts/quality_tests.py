import psycopg2

def run_tests():
    conn = psycopg2.connect("dbname=airflow user=airflow password=airflow host=postgres")
    cur = conn.cursor()

    # Teste 1: Verificar códigos IBGE
    cur.execute("SELECT cod_ibge FROM populacao WHERE cod_ibge NOT IN (SELECT cod_ibge FROM municipios)")
    if cur.fetchall():
        raise ValueError("Códigos IBGE inválidos encontrados na tabela populacao")

    # Teste 2: Verificar duplicatas
    cur.execute("SELECT cod_ibge, ano, COUNT(*) FROM populacao GROUP BY cod_ibge, ano HAVING COUNT(*) > 1")
    if cur.fetchall():
        raise ValueError("Duplicatas encontradas na tabela populacao")

    # Teste 3: Valores fora do esperado
    cur.execute("SELECT * FROM populacao WHERE populacao < 0")
    if cur.fetchall():
        raise ValueError("População negativa encontrada")

    conn.close()