# Arquitetura Medalhão

## O que é a Arquitetura Medalhão?

A **Arquitetura Medalhão** (Medallion Architecture) é um padrão de organização de dados em
camadas para Lakehouses, onde cada camada representa um nível crescente de qualidade e
refinamento dos dados:

```
BRUTO ──────────────────────────────────────────────► ANALÍTICO
LANDING → BRONZE → SILVER → GOLD
```

---

## Camada LANDING

**Notebook:** `01_landing.py`

### Objetivo
Extrair os dados brutos da origem (Supabase PostgreSQL) e armazená-los no Databricks
exatamente como vieram, sem nenhuma transformação.

### Implementação
- Conexão via **JDBC PostgreSQL** (driver nativo no Databricks)
- Lê cada tabela com `spark.read.jdbc()`
- Salva como **CSV** em `dbfs:/FileStore/tables/landing/{tabela}/`
- Registra **external tables** no schema `landing` do Databricks
- Validação: contagem de registros deve ser igual à do Supabase

### Tabelas
```
landing.regiao, landing.estado, landing.municipio, landing.marca, landing.modelo,
landing.cliente, landing.endereco, landing.telefone, landing.carro,
landing.apolice, landing.sinistro
```

### Por que CSV?
O CSV é o formato de troca universal. Permite que a origem seja qualquer sistema
(banco relacional, APIs, arquivos) sem dependência de formato específico.

---

## Camada BRONZE

**Notebook:** `02_bronze.py`

### Objetivo
Ler os arquivos CSV da camada Landing e converter para **Delta Lake**, habilitando
transações ACID, versionamento e time travel.

### Implementação
- Lê do schema `landing` (external tables CSV)
- Converte para **managed Delta tables** no schema `bronze`
- Modo `overwrite` para re-execuções idempotentes
- Valida com `DESCRIBE HISTORY` (transaction log)

### Benefícios do Delta Lake no Bronze
| Benefício | Descrição |
|---|---|
| **ACID** | Leituras e escritas atômicas e consistentes |
| **Time Travel** | Leitura de versões anteriores da tabela |
| **Transaction Log** | Auditoria completa de todas as operações |
| **Schema Evolution** | Alterações de schema sem recriar tabelas |

### Exemplo — Transaction Log
```sql
DESCRIBE HISTORY bronze.apolice;
-- version | timestamp | operation | operationMetrics
-- 0       | 2025-...  | WRITE     | numFiles=1, numRows=10000
```

---

## Camada SILVER

**Notebook:** `03_silver.py`

### Objetivo
Aplicar regras de **Data Quality** sobre os dados Bronze, garantindo que apenas dados
válidos, tipados e sem duplicatas cheguem para as análises.

### Regras de Data Quality aplicadas

#### Genéricas (todas as tabelas)
- `dropDuplicates()` — remove linhas exatamente iguais
- `dropna(subset=[pk])` — remove linhas com PK nula

#### Específicas por tabela

| Tabela | Regras |
|---|---|
| `cliente` | cpf com 11 dígitos, sexo in ('M','F'), dt_nascimento ≤ hoje |
| `estado` | sigla_uf com exatamente 2 caracteres |
| `carro` | ano entre 1900 e ano atual |
| `endereco` | nr_endereco > 0 |
| `telefone` | nr_telefone com ≥ 8 dígitos |
| `apolice` | vl_cobertura > 0, vl_franquia ≥ 0, dt_fim > dt_inicio |
| `sinistro` | dt_sinistro não pode ser data futura |

#### Transformações de tipo
```python
.withColumn("dt_nascimento",    F.col("dt_nascimento").cast(DateType()))
.withColumn("dt_inicio_vigencia", F.col("dt_inicio_vigencia").cast(TimestampType()))
.withColumn("vl_cobertura",     F.col("vl_cobertura").cast(DecimalType(15, 2)))
.withColumn("placa",            F.trim(F.col("placa")))  # remove espaços
```

#### Colunas derivadas

| Tabela | Coluna | Fórmula |
|---|---|---|
| `cliente` | `nr_idade` | `datediff(today, dt_nascimento) / 365.25` |
| `cliente` | `faixa_etaria` | Menor de 18 / 18-30 / 31-45 / 46-60 / Acima de 60 |
| `carro` | `faixa_ano` | Antes de 2000 / 2000-2010 / 2011-2020 / 2021 em diante |
| `apolice` | `dias_vigencia` | `datediff(dt_fim, dt_inicio)` |

### Relatório de Data Quality
O notebook gera um relatório consolidado comparando contagens Bronze × Silver:

```
======================================================
RELATÓRIO CONSOLIDADO — DATA QUALITY
Tabela         Bronze   Silver  Removidos    % DQ
------------------------------------------------------
  cliente       20010    19987        23      0.1%
  apolice       10000     9998         2      0.0%
  sinistro      10000    10000         0      0.0%
  ...
```

---

## Camada GOLD

**Notebook:** `04_gold.py`

### Objetivo
Modelar os dados Silver segundo a metodologia **Ralph Kimball** (modelo dimensional),
criando dimensões e tabelas fato prontas para análise e BI.

> Detalhamento completo em [Modelo Dimensional (Gold)](gold_kimball.md).

### Tabelas criadas
```
gold.dim_data           gold.fato_apolice
gold.dim_cliente        gold.fato_sinistro
gold.dim_veiculo
gold.dim_localizacao
```

---

## Comparação entre camadas

| Aspecto | Landing | Bronze | Silver | Gold |
|---|---|---|---|---|
| **Formato** | CSV | Delta | Delta | Delta |
| **Qualidade** | Bruto | Bruto | Tratado | Modelado |
| **Tipos** | String (inferidos) | Inferidos | Corretos | Corretos |
| **Duplicatas** | Possível | Possível | Removidas | Removidas |
| **Uso** | Ingestão | Auditoria | Análise | BI / Relatórios |
| **Schema** | External | Managed | Managed | Managed |
