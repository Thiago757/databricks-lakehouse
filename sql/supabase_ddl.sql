-- =============================================================================
-- Trabalho 3 — SeguroDB: DDL para Supabase (PostgreSQL)
-- Domínio: Seguro Automotivo
-- Execução: SQL Editor do Supabase (https://supabase.com)
-- =============================================================================

-- Ordem de criação respeita as dependências de FK

CREATE TABLE IF NOT EXISTS regiao (
    cd_regiao   INTEGER PRIMARY KEY,
    nm_regiao   VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS estado (
    cd_estado   INTEGER PRIMARY KEY,
    cd_regiao   INTEGER NOT NULL REFERENCES regiao(cd_regiao),
    nm_estado   VARCHAR(100) NOT NULL,
    sigla_uf    CHAR(2) NOT NULL
);

CREATE TABLE IF NOT EXISTS municipio (
    cd_municipio    INTEGER PRIMARY KEY,
    nm_municipio    VARCHAR(100) NOT NULL,
    cd_estado       INTEGER NOT NULL REFERENCES estado(cd_estado)
);

CREATE TABLE IF NOT EXISTS marca (
    cd_marca    INTEGER PRIMARY KEY,
    nm_marca    VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS modelo (
    cd_modelo   INTEGER PRIMARY KEY,
    cd_marca    INTEGER NOT NULL REFERENCES marca(cd_marca),
    nm_modelo   VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS cliente (
    cd_cliente      INTEGER PRIMARY KEY,
    nome            VARCHAR(150) NOT NULL,
    cpf             VARCHAR(11) NOT NULL,
    sexo            CHAR(1) NOT NULL CHECK (sexo IN ('M', 'F')),
    dt_nascimento   DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS endereco (
    cd_cliente      INTEGER NOT NULL REFERENCES cliente(cd_cliente),
    cd_municipio    INTEGER NOT NULL REFERENCES municipio(cd_municipio),
    ds_endereco     VARCHAR(200) NOT NULL,
    nr_endereco     INTEGER NOT NULL,
    bairro          VARCHAR(100) NOT NULL,
    PRIMARY KEY (cd_cliente)
);

CREATE TABLE IF NOT EXISTS telefone (
    cd_cliente  INTEGER NOT NULL REFERENCES cliente(cd_cliente),
    nr_telefone VARCHAR(20) NOT NULL,
    PRIMARY KEY (cd_cliente, nr_telefone)
);

CREATE TABLE IF NOT EXISTS carro (
    placa       VARCHAR(20) PRIMARY KEY,
    cd_modelo   INTEGER NOT NULL REFERENCES modelo(cd_modelo),
    chassi      VARCHAR(50) NOT NULL,
    ano         INTEGER NOT NULL,
    cor         VARCHAR(30) NOT NULL
);

CREATE TABLE IF NOT EXISTS apolice (
    cd_apolice          INTEGER PRIMARY KEY,
    cd_cliente          INTEGER NOT NULL REFERENCES cliente(cd_cliente),
    dt_inicio_vigencia  TIMESTAMP NOT NULL,
    dt_fim_vigencia     TIMESTAMP NOT NULL,
    vl_cobertura        NUMERIC(15, 2) NOT NULL,
    vl_franquia         NUMERIC(15, 2) NOT NULL,
    placa               VARCHAR(20) NOT NULL REFERENCES carro(placa)
);

CREATE TABLE IF NOT EXISTS sinistro (
    cd_sinistro     INTEGER PRIMARY KEY,
    placa           VARCHAR(20) NOT NULL REFERENCES carro(placa),
    dt_sinistro     TIMESTAMP NOT NULL,
    local_sinistro  INTEGER NOT NULL REFERENCES municipio(cd_municipio),
    condutor        VARCHAR(100) NOT NULL
);
