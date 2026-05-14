# Trabalho 3 — Lakehouse com Databricks e Arquitetura Medalhão

## Objetivo

Construir um pipeline de dados no **Databricks Free Edition** implementando a
**Arquitetura Medalhão** completa — Landing → Bronze → Silver → Gold — com
orquestração via **Jobs & Pipelines**.

## Contexto

Este trabalho é complemento dos Trabalhos 1 e 2. Utiliza o mesmo domínio de dados
(**SeguroDB — Seguro Automotivo**), agora extraindo do **Supabase (PostgreSQL)** e
processando no Databricks com Delta Lake.

## Tecnologias

| Tecnologia | Papel |
|---|---|
| **Databricks Free Edition** | Plataforma de processamento e armazenamento (PaaS) |
| **Apache Spark** | Motor de processamento distribuído (incluso no Databricks) |
| **Delta Lake** | Formato de tabelas ACID com time travel |
| **Supabase (PostgreSQL)** | Banco de dados fonte |
| **Jobs & Pipelines** | Orquestração sequencial dos notebooks |

## Arquitetura do Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SUPABASE (PostgreSQL)                        │
│          11 tabelas │ ~85.000 registros │ Seguro Automotivo         │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  JDBC
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     LANDING  (schema: landing)                      │
│   External Tables em CSV │ dados brutos da origem                   │
│   Notebook: 01_landing.py                                           │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  Delta Write
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      BRONZE  (schema: bronze)                       │
│   Managed Delta Tables │ dados brutos em formato otimizado          │
│   Notebook: 02_bronze.py                                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  Data Quality
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      SILVER  (schema: silver)                       │
│   Managed Delta Tables │ dados tratados, confiáveis, tipados        │
│   Notebook: 03_silver.py                                            │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  Kimball Dimensional
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       GOLD  (schema: gold)                          │
│   4 Dimensões + 2 Fatos │ pronto para análise e BI                 │
│   Notebook: 04_gold.py                                              │
└─────────────────────────────────────────────────────────────────────┘
```

## Tabelas do SeguroDB

```
REGIAO (5)
  └── ESTADO (27)
        └── MUNICIPIO (5.570)
              └── ENDERECO → CLIENTE (20.010)
                                ├── TELEFONE (20.010)
                                └── APOLICE (10.000)
                                      │
                                      └── SINISTRO (10.000)
                                            │
MARCA (10)                          CARRO (10.002)
  └── MODELO (100) ─────────────────────┘
```

## Jobs & Pipelines

Todos os notebooks são encadeados num único **Job** no Databricks Workflows,
executando sequencialmente:

```
Task 1: 01_landing  →  Task 2: 02_bronze  →  Task 3: 03_silver  →  Task 4: 04_gold
```
