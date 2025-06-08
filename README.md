# Desafio Técnico Jetsales - Engenheiro de Dados Pleno

## Visão Geral
Este projeto implementa um pipeline de dados integrado com foco em municípios brasileiros, utilizando bases públicas do IBGE, CNES e INEP. O pipeline é orquestrado com Apache Airflow, carrega dados em um banco PostgreSQL e inclui testes de qualidade.

## Bases Escolhidas
- **IBGE - Estimativas Populacionais**: População por município.
- **CNES**: Quantidade de leitos e unidades de saúde.
- **INEP - Censo Escolar**: Matrículas e IDEB.

## Modelagem
A modelagem em estrela centraliza a tabela `municipios` (chave: `cod_ibge`) com tabelas de fatos (`populacao`, `saude_cnes`, `educacao_inep`) relacionadas por chaves estrangeiras.

## Como Executar
1. Instale o Docker e o Docker Compose.
2. Clone este repositório.
3. Execute `docker-compose up -d`.
4. Acesse o Airflow em `http://localhost:8080` (usuário/senha: airflow/airflow).
5. Ative a DAG `pipeline_municipios`.

## Queries Analíticas
Exemplo: Leitos por 100 mil habitantes:
```sql
SELECT m.nome_municipio, (s.leitos * 100000.0 / p.populacao) AS leitos_por_100mil
FROM municipios m
JOIN populacao p ON m.cod_ibge = p.cod_ibge
JOIN saude_cnes s ON m.cod_ibge = s.cod_ibge
WHERE p.ano = s.ano;