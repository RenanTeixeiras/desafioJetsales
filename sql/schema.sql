CREATE TABLE IF NOT EXISTS municipios (
    cod_ibge VARCHAR(7) PRIMARY KEY,
    nome_municipio VARCHAR(100),
    uf VARCHAR(2),
    regiao VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS populacao (
    cod_ibge VARCHAR(7),
    ano INTEGER,
    populacao INTEGER,
    PRIMARY KEY (cod_ibge, ano),
    FOREIGN KEY (cod_ibge) REFERENCES municipios(cod_ibge)
);

CREATE TABLE IF NOT EXISTS saude_cnes (
    cod_ibge VARCHAR(7),
    ano INTEGER,
    leitos INTEGER,
    unidades_saude INTEGER,
    PRIMARY KEY (cod_ibge, ano),
    FOREIGN KEY (cod_ibge) REFERENCES municipios(cod_ibge)
);

CREATE TABLE IF NOT EXISTS educacao_inep (
    cod_ibge VARCHAR(7),
    ano INTEGER,
    matriculas INTEGER,
    ideb DECIMAL(4,2),
    PRIMARY KEY (cod_ibge, ano),
    FOREIGN KEY (cod_ibge) REFERENCES municipios(cod_ibge)
);