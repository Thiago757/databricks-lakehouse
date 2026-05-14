# Trabalho 3 — Lakehouse com Databricks e Arquitetura Medalhão

Pipeline de Engenharia de Dados implementando a **Arquitetura Medalhão** completa
(Landing → Bronze → Silver → Gold) no **Databricks Free Edition**, usando dados do
domínio de Seguro Automotivo (**SeguroDB**) extraídos do **Supabase (PostgreSQL)**.

> **Nota:** Este é o Trabalho 3, complemento dos Trabalhos 1 e 2. Cada trabalho possui repositório, documentação e notebooks independentes.

## Arquitetura

```
Supabase (PostgreSQL)
    11 tabelas — ~85k registros
        │
        │  JDBC (Notebook 01)
        ▼
schema: landing
    11 external tables (CSV no DBFS)
        │
        │  Delta Lake (Notebook 02)
        ▼
schema: bronze
    11 managed Delta tables (dados brutos)
        │
        │  Data Quality (Notebook 03)
        ▼
schema: silver
    11 managed Delta tables (dados tratados e confiáveis)
        │
        │  Ralph Kimball (Notebook 04)
        ▼
schema: gold
    4 dimensões + 2 tabelas fato (modelo dimensional)
        │
        ├── dim_data
        ├── dim_cliente
        ├── dim_veiculo
        ├── dim_localizacao
        ├── fato_apolice
        └── fato_sinistro
```

## Pré-requisitos

| Ferramenta | Versão | Link |
|---|---|---|
| **Supabase** | Free Tier | [supabase.com](https://supabase.com) |
| **Databricks** | Free Edition (Community) | [community.cloud.databricks.com](https://community.cloud.databricks.com) |
| **Python** | 3.10+ (para o script de carga) | [python.org](https://www.python.org/) |
| **psycopg2-binary** | latest | `pip install psycopg2-binary pandas` |

## Configuração — Supabase

```bash
# 1. Crie uma conta em supabase.com → New Project
# 2. Anote: Host (db.<ref>.supabase.co), Senha e Database (postgres)

# 3. No SQL Editor do Supabase, execute o DDL:
#    sql/supabase_ddl.sql

# 4. Edite o script de carga com suas credenciais:
#    sql/supabase_load_data.py
#      SUPABASE_HOST = "db.<ref>.supabase.co"
#      SUPABASE_PASSWORD = "<sua_senha>"

# 5. Instale as dependências e rode o script:
pip install psycopg2-binary pandas
python sql/supabase_load_data.py
```

## Configuração — Databricks

```
1. Crie conta em: https://community.cloud.databricks.com
2. Crie um cluster: Compute → Create cluster → Runtime 15.x LTS
3. Importe os notebooks: Workspace → Import → selecione cada .py em /notebooks/
4. No notebook 01_landing, preencha os widgets:
   - SUPABASE_HOST:     db.<ref>.supabase.co
   - SUPABASE_PASSWORD: <sua_senha>
```

## Execução (Jobs & Pipelines)

Configure um **Job** no Databricks Workflows com 4 tasks sequenciais:

| Task | Notebook            | Depende de |
|------|---------------------|------------|
| 1    | `01_landing.py`     | —          |
| 2    | `02_bronze.py`      | Task 1     |
| 3    | `03_silver.py`      | Task 2     |
| 4    | `04_gold.py`        | Task 3     |

```
Workflows → Create Job → Add Task (para cada notebook) → Run Now
```

## Estrutura do projeto

```
trabalho-3-databricks-lakehouse/
├── README.md
├── .gitignore
├── mkdocs.yml                      # Documentação MkDocs
├── notebooks/
│   ├── 01_landing.py               # Supabase → LANDING (CSV)
│   ├── 02_bronze.py                # LANDING → BRONZE (Delta Lake)
│   ├── 03_silver.py                # BRONZE → SILVER (Data Quality)
│   └── 04_gold.py                  # SILVER → GOLD (Kimball dimensional)
├── sql/
│   ├── supabase_ddl.sql            # DDL — 11 tabelas no Supabase
│   └── supabase_load_data.py       # Script de carga dos CSVs
├── data/
│   └── (CSVs do SeguroDB — 11 arquivos, ~85k registros)
└── docs/
    ├── index.md                    # Visão geral e arquitetura
    ├── setup.md                    # Guia de configuração
    ├── medallion.md                # Arquitetura Medalhão (cada camada)
    └── gold_kimball.md             # Modelo dimensional Ralph Kimball
```

## Modelo Dimensional (Gold)

### Dimensões

| Tabela            | SK                  | Fonte Silver              | Colunas derivadas          |
|-------------------|---------------------|---------------------------|----------------------------|
| `dim_data`        | sk_data (YYYYMMDD)  | apolice + sinistro (datas)| nm_mes, is_fim_semana, trimestre |
| `dim_cliente`     | sk_cliente          | cliente                   | faixa_etaria, nr_idade     |
| `dim_veiculo`     | sk_veiculo          | carro + modelo + marca    | nm_modelo, nm_marca, faixa_ano |
| `dim_localizacao` | sk_localizacao      | municipio + estado + regiao | nm_estado, sigla_uf, nm_regiao |

### Fatos

| Tabela          | Grão               | Métricas                                   |
|-----------------|--------------------|--------------------------------------------|
| `fato_apolice`  | 1 linha/apólice    | vl_cobertura, vl_franquia, dias_vigencia   |
| `fato_sinistro` | 1 linha/sinistro   | qt_sinistros (= 1)                         |

## Documentação

```bash
# Para visualizar localmente (requer mkdocs)
pip install mkdocs mkdocs-material
mkdocs serve
# Acesse: http://localhost:8000
```

## Tabelas do SeguroDB

| Tabela    | Registros | Descrição                          |
|-----------|-----------|-------------------------------------|
| regiao    | 5         | Regiões do Brasil                   |
| estado    | 27        | Estados (UF)                        |
| municipio | 5.570     | Municípios brasileiros              |
| marca     | 10        | Marcas de veículos                  |
| modelo    | 100       | Modelos de veículos por marca       |
| cliente   | 20.010    | Clientes com dados pessoais         |
| endereco  | 20.010    | Endereços dos clientes              |
| telefone  | 20.010    | Telefones dos clientes              |
| carro     | 10.002    | Veículos segurados                  |
| apolice   | 10.000    | Apólices de seguro automotivo       |
| sinistro  | 10.000    | Sinistros registrados               |

## Tecnologias

- **Databricks Free Edition** — Plataforma de dados na nuvem (PaaS)
- **Delta Lake** — Formato ACID com time travel e transaction log
- **Apache Spark** — Motor de processamento distribuído (incluso no Databricks)
- **Supabase** — Banco de dados PostgreSQL gerenciado na nuvem
- **Ralph Kimball** — Metodologia de modelagem dimensional (Gold)
- **Jobs & Pipelines** — Orquestração sequencial no Databricks Workflows
- **MkDocs Material** — Documentação do projeto

## Referências

### Documentações Oficiais

| Tecnologia | Link |
|---|---|
| Databricks Free Edition | [docs.databricks.com](https://docs.databricks.com) |
| Delta Lake | [docs.delta.io](https://docs.delta.io) |
| Apache Spark | [spark.apache.org/docs/latest](https://spark.apache.org/docs/latest) |
| Supabase | [supabase.com/docs](https://supabase.com/docs) |
| MkDocs Material | [squidfunk.github.io/mkdocs-material](https://squidfunk.github.io/mkdocs-material) |

### Referências Bibliográficas (ABNT)

KIMBALL, Ralph; ROSS, Margy. **The Data Warehouse Toolkit: The Definitive Guide to Dimensional Modeling**. 3. ed. Indianapolis: John Wiley & Sons, 2013.

DATABRICKS. **Databricks Documentation**. Disponível em: <https://docs.databricks.com>. Acesso em: 14 maio 2026.

DELTA LAKE. **Delta Lake Documentation**. Disponível em: <https://docs.delta.io>. Acesso em: 14 maio 2026.

APACHE SOFTWARE FOUNDATION. **Apache Spark Documentation**. Disponível em: <https://spark.apache.org/docs/latest>. Acesso em: 14 maio 2026.

SUPABASE. **Supabase Documentation**. Disponível em: <https://supabase.com/docs>. Acesso em: 14 maio 2026.

SQUIDFUNK. **MkDocs Material Documentation**. Disponível em: <https://squidfunk.github.io/mkdocs-material>. Acesso em: 14 maio 2026.
