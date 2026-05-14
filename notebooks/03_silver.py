# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 03 — Data Quality: BRONZE → SILVER
# MAGIC
# MAGIC Lê as tabelas Delta do schema `bronze`, aplica regras de **Data Quality**
# MAGIC e grava as tabelas tratadas no schema `silver`.
# MAGIC
# MAGIC **Regras aplicadas:**
# MAGIC - Remoção de duplicatas
# MAGIC - Remoção de nulos em PKs/FKs
# MAGIC - Cast de tipos corretos (datas, decimais, inteiros)
# MAGIC - Trim de strings (ex: placa vem com espaços do CSV fonte)
# MAGIC - Validações de negócio (sexo, valores, anos)
# MAGIC - Colunas derivadas (faixa_etaria, dias_vigencia)
# MAGIC
# MAGIC **Fluxo:**
# MAGIC ```
# MAGIC bronze.{tabela}  (Delta Lake — dados brutos)
# MAGIC     ──DQ Rules──► DataFrame tratado
# MAGIC     ──Delta Write──► silver.{tabela}  (Delta Lake — dados confiáveis)
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Imports e configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DecimalType, DateType, TimestampType

BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

print(f"Origem : {BRONZE_SCHEMA}")
print(f"Destino: {SILVER_SCHEMA} (Delta Lake — tratado)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Criar schema SILVER

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
print(f"Schema '{SILVER_SCHEMA}' pronto.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Funções auxiliares de Data Quality

# COMMAND ----------

def relatorio_dq(tabela, antes, depois):
    removidos = antes - depois
    pct = (removidos / antes * 100) if antes > 0 else 0
    print(f"  {tabela:12s} | antes={antes:>6} | depois={depois:>6} | removidos={removidos:>5} ({pct:.1f}%)")

def salvar_silver(df, tabela):
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{SILVER_SCHEMA}.{tabela}")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Data Quality por tabela

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.1 REGIAO

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.regiao")
antes = df.count()

df = (df
    .dropDuplicates(["cd_regiao"])
    .dropna(subset=["cd_regiao", "nm_regiao"])
    .withColumn("cd_regiao", F.col("cd_regiao").cast(IntegerType()))
    .withColumn("nm_regiao", F.trim(F.col("nm_regiao")))
)

salvar_silver(df, "regiao")
relatorio_dq("regiao", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.2 ESTADO

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.estado")
antes = df.count()

df = (df
    .dropDuplicates(["cd_estado"])
    .dropna(subset=["cd_estado", "cd_regiao", "nm_estado", "sigla_uf"])
    .withColumn("cd_estado", F.col("cd_estado").cast(IntegerType()))
    .withColumn("cd_regiao", F.col("cd_regiao").cast(IntegerType()))
    .withColumn("nm_estado", F.trim(F.col("nm_estado")))
    .withColumn("sigla_uf",  F.trim(F.col("sigla_uf")))
    .filter(F.length(F.col("sigla_uf")) == 2)
)

salvar_silver(df, "estado")
relatorio_dq("estado", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.3 MUNICIPIO

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.municipio")
antes = df.count()

df = (df
    .dropDuplicates(["cd_municipio"])
    .dropna(subset=["cd_municipio", "nm_municipio", "cd_estado"])
    .withColumn("cd_municipio", F.col("cd_municipio").cast(IntegerType()))
    .withColumn("cd_estado",    F.col("cd_estado").cast(IntegerType()))
    .withColumn("nm_municipio", F.trim(F.col("nm_municipio")))
)

salvar_silver(df, "municipio")
relatorio_dq("municipio", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.4 MARCA

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.marca")
antes = df.count()

df = (df
    .dropDuplicates(["cd_marca"])
    .dropna(subset=["cd_marca", "nm_marca"])
    .withColumn("cd_marca", F.col("cd_marca").cast(IntegerType()))
    .withColumn("nm_marca", F.trim(F.col("nm_marca")))
)

salvar_silver(df, "marca")
relatorio_dq("marca", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.5 MODELO

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.modelo")
antes = df.count()

df = (df
    .dropDuplicates(["cd_modelo"])
    .dropna(subset=["cd_modelo", "cd_marca", "nm_modelo"])
    .withColumn("cd_modelo", F.col("cd_modelo").cast(IntegerType()))
    .withColumn("cd_marca",  F.col("cd_marca").cast(IntegerType()))
    .withColumn("nm_modelo", F.trim(F.col("nm_modelo")))
)

salvar_silver(df, "modelo")
relatorio_dq("modelo", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.6 CLIENTE

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.cliente")
antes = df.count()

df = (df
    .dropDuplicates(["cd_cliente"])
    .dropna(subset=["cd_cliente", "nome", "cpf", "sexo", "dt_nascimento"])
    .withColumn("cd_cliente",    F.col("cd_cliente").cast(IntegerType()))
    .withColumn("nome",          F.trim(F.col("nome")))
    .withColumn("cpf",           F.trim(F.col("cpf")))
    .withColumn("sexo",          F.trim(F.col("sexo")))
    .withColumn("dt_nascimento", F.col("dt_nascimento").cast(DateType()))
    # Valida sexo
    .filter(F.col("sexo").isin("M", "F"))
    # Valida CPF com 11 dígitos
    .filter(F.length(F.regexp_replace(F.col("cpf"), "[^0-9]", "")) == 11)
    # Valida data de nascimento: não pode ser no futuro
    .filter(F.col("dt_nascimento") <= F.current_date())
    # Coluna derivada: idade em anos
    .withColumn("nr_idade", F.floor(F.datediff(F.current_date(), F.col("dt_nascimento")) / 365.25).cast(IntegerType()))
    # Coluna derivada: faixa etária
    .withColumn("faixa_etaria", F.when(F.col("nr_idade") < 18, "Menor de 18")
                                 .when(F.col("nr_idade") <= 30, "18-30")
                                 .when(F.col("nr_idade") <= 45, "31-45")
                                 .when(F.col("nr_idade") <= 60, "46-60")
                                 .otherwise("Acima de 60"))
)

salvar_silver(df, "cliente")
relatorio_dq("cliente", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.7 ENDERECO

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.endereco")
antes = df.count()

df = (df
    .dropDuplicates(["cd_cliente"])
    .dropna(subset=["cd_cliente", "cd_municipio", "ds_endereco", "nr_endereco", "bairro"])
    .withColumn("cd_cliente",   F.col("cd_cliente").cast(IntegerType()))
    .withColumn("cd_municipio", F.col("cd_municipio").cast(IntegerType()))
    .withColumn("ds_endereco",  F.trim(F.col("ds_endereco")))
    .withColumn("nr_endereco",  F.col("nr_endereco").cast(IntegerType()))
    .withColumn("bairro",       F.trim(F.col("bairro")))
    .filter(F.col("nr_endereco") > 0)
)

salvar_silver(df, "endereco")
relatorio_dq("endereco", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.8 TELEFONE

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.telefone")
antes = df.count()

df = (df
    .dropDuplicates(["cd_cliente", "nr_telefone"])
    .dropna(subset=["cd_cliente", "nr_telefone"])
    .withColumn("cd_cliente",  F.col("cd_cliente").cast(IntegerType()))
    .withColumn("nr_telefone", F.regexp_replace(F.trim(F.col("nr_telefone")), "[^0-9]", ""))
    .filter(F.length(F.col("nr_telefone")) >= 8)
)

salvar_silver(df, "telefone")
relatorio_dq("telefone", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.9 CARRO

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.carro")
antes = df.count()

df = (df
    .dropDuplicates(["placa"])
    .dropna(subset=["placa", "cd_modelo", "chassi", "ano", "cor"])
    .withColumn("placa",     F.trim(F.col("placa")))
    .withColumn("cd_modelo", F.col("cd_modelo").cast(IntegerType()))
    .withColumn("chassi",    F.trim(F.col("chassi")))
    .withColumn("ano",       F.col("ano").cast(IntegerType()))
    .withColumn("cor",       F.trim(F.upper(F.col("cor"))))
    # Ano válido: 1900 a ano atual
    .filter((F.col("ano") >= 1900) & (F.col("ano") <= F.year(F.current_date())))
    # Coluna derivada: faixa de ano do veículo
    .withColumn("faixa_ano", F.when(F.col("ano") < 2000, "Antes de 2000")
                              .when(F.col("ano") <= 2010, "2000-2010")
                              .when(F.col("ano") <= 2020, "2011-2020")
                              .otherwise("2021 em diante"))
)

salvar_silver(df, "carro")
relatorio_dq("carro", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.10 APOLICE

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.apolice")
antes = df.count()

df = (df
    .dropDuplicates(["cd_apolice"])
    .dropna(subset=["cd_apolice", "cd_cliente", "placa", "dt_inicio_vigencia",
                    "dt_fim_vigencia", "vl_cobertura", "vl_franquia"])
    .withColumn("cd_apolice",         F.col("cd_apolice").cast(IntegerType()))
    .withColumn("cd_cliente",         F.col("cd_cliente").cast(IntegerType()))
    .withColumn("placa",              F.trim(F.col("placa")))
    .withColumn("dt_inicio_vigencia", F.col("dt_inicio_vigencia").cast(TimestampType()))
    .withColumn("dt_fim_vigencia",    F.col("dt_fim_vigencia").cast(TimestampType()))
    .withColumn("vl_cobertura",       F.col("vl_cobertura").cast(DecimalType(15, 2)))
    .withColumn("vl_franquia",        F.col("vl_franquia").cast(DecimalType(15, 2)))
    # Validações de negócio
    .filter(F.col("vl_cobertura") > 0)
    .filter(F.col("vl_franquia") >= 0)
    .filter(F.col("dt_fim_vigencia") > F.col("dt_inicio_vigencia"))
    # Coluna derivada: duração da apólice em dias
    .withColumn("dias_vigencia",
        F.datediff(
            F.col("dt_fim_vigencia").cast(DateType()),
            F.col("dt_inicio_vigencia").cast(DateType())
        ).cast(IntegerType())
    )
)

salvar_silver(df, "apolice")
relatorio_dq("apolice", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ### 4.11 SINISTRO

# COMMAND ----------

df = spark.read.table(f"{BRONZE_SCHEMA}.sinistro")
antes = df.count()

df = (df
    .dropDuplicates(["cd_sinistro"])
    .dropna(subset=["cd_sinistro", "placa", "dt_sinistro", "local_sinistro", "condutor"])
    .withColumn("cd_sinistro",    F.col("cd_sinistro").cast(IntegerType()))
    .withColumn("placa",          F.trim(F.col("placa")))
    .withColumn("dt_sinistro",    F.col("dt_sinistro").cast(TimestampType()))
    .withColumn("local_sinistro", F.col("local_sinistro").cast(IntegerType()))
    .withColumn("condutor",       F.trim(F.upper(F.col("condutor"))))
    # Data do sinistro não pode ser futura
    .filter(F.col("dt_sinistro") <= F.current_timestamp())
)

salvar_silver(df, "sinistro")
relatorio_dq("sinistro", antes, df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Relatório consolidado de Data Quality

# COMMAND ----------

print("=" * 65)
print("RELATÓRIO CONSOLIDADO — DATA QUALITY (BRONZE → SILVER)")
print("=" * 65)
print(f"{'Tabela':<14} {'Bronze':>8} {'Silver':>8} {'Removidos':>10} {'% DQ':>7}")
print("-" * 65)

tabelas = ["regiao", "estado", "municipio", "marca", "modelo",
           "cliente", "endereco", "telefone", "carro", "apolice", "sinistro"]

total_bronze = 0
total_silver = 0

for t in tabelas:
    cnt_b = spark.sql(f"SELECT count(*) as c FROM {BRONZE_SCHEMA}.{t}").collect()[0]["c"]
    cnt_s = spark.sql(f"SELECT count(*) as c FROM {SILVER_SCHEMA}.{t}").collect()[0]["c"]
    rem = cnt_b - cnt_s
    pct = f"{(1 - cnt_s/cnt_b)*100:.1f}%" if cnt_b > 0 else "N/A"
    total_bronze += cnt_b
    total_silver += cnt_s
    print(f"  {t:<12} {cnt_b:>8} {cnt_s:>8} {rem:>10} {pct:>7}")

print("-" * 65)
pct_total = f"{(1 - total_silver/total_bronze)*100:.1f}%" if total_bronze > 0 else "N/A"
print(f"  {'TOTAL':<12} {total_bronze:>8} {total_silver:>8} {total_bronze-total_silver:>10} {pct_total:>7}")
print("=" * 65)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Amostra dos dados Silver

# COMMAND ----------

print("=== Amostra: silver.cliente (com faixa_etaria e nr_idade) ===")
spark.sql("""
    SELECT cd_cliente, nome, sexo, dt_nascimento, nr_idade, faixa_etaria
    FROM silver.cliente
    LIMIT 5
""").show(truncate=False)

print("=== Amostra: silver.apolice (com dias_vigencia) ===")
spark.sql("""
    SELECT cd_apolice, cd_cliente, placa, vl_cobertura, vl_franquia, dias_vigencia
    FROM silver.apolice
    LIMIT 5
""").show(truncate=False)

print("=== Amostra: silver.carro (com faixa_ano) ===")
spark.sql("""
    SELECT placa, ano, cor, faixa_ano
    FROM silver.carro
    LIMIT 5
""").show(truncate=False)
