# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 04 — Modelagem: SILVER → GOLD (Ralph Kimball)
# MAGIC
# MAGIC Lê as tabelas confiáveis do schema `silver` e constrói o **modelo dimensional**
# MAGIC (Ralph Kimball) no schema `gold`, com **4 dimensões** e **2 tabelas fato**.
# MAGIC
# MAGIC **Modelo Dimensional:**
# MAGIC ```
# MAGIC DIMENSÕES:
# MAGIC   gold.dim_data         — Calendário (gerado a partir das datas dos fatos)
# MAGIC   gold.dim_cliente      — Clientes (com faixa etária e idade)
# MAGIC   gold.dim_veiculo      — Veículos (carro + modelo + marca)
# MAGIC   gold.dim_localizacao  — Geografia (municipio + estado + regiao)
# MAGIC
# MAGIC FATOS:
# MAGIC   gold.fato_apolice     — Apólices de seguro (métricas: cobertura, franquia, vigência)
# MAGIC   gold.fato_sinistro    — Sinistros registrados (métricas: contagem)
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Imports e configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window

SILVER_SCHEMA = "silver"
GOLD_SCHEMA   = "gold"

print(f"Origem : {SILVER_SCHEMA}")
print(f"Destino: {GOLD_SCHEMA} (Modelo Dimensional — Ralph Kimball)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Criar schema GOLD

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
print(f"Schema '{GOLD_SCHEMA}' pronto.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. DIM_DATA — Dimensão Calendário
# MAGIC
# MAGIC Gerada a partir de todas as datas presentes nas tabelas `apolice` e `sinistro`.
# MAGIC `sk_data` = chave inteira no formato **YYYYMMDD**.

# COMMAND ----------

# Coleta todas as datas distintas dos fatos
df_apolice_datas = (
    spark.read.table(f"{SILVER_SCHEMA}.apolice")
    .select(
        F.col("dt_inicio_vigencia").cast("date").alias("dt"),
        F.col("dt_fim_vigencia").cast("date").alias("dt2"),
    )
    .select(
        F.explode(F.array(F.col("dt"), F.col("dt2"))).alias("dt_data")
    )
)

df_sinistro_datas = (
    spark.read.table(f"{SILVER_SCHEMA}.sinistro")
    .select(F.col("dt_sinistro").cast("date").alias("dt_data"))
)

df_datas = (
    df_apolice_datas
    .union(df_sinistro_datas)
    .dropna()
    .dropDuplicates(["dt_data"])
)

# Mapas para nome de mês e dia da semana usando expressões CASE
meses_case = (
    F.when(F.month("dt_data") == 1,  "Janeiro")
     .when(F.month("dt_data") == 2,  "Fevereiro")
     .when(F.month("dt_data") == 3,  "Março")
     .when(F.month("dt_data") == 4,  "Abril")
     .when(F.month("dt_data") == 5,  "Maio")
     .when(F.month("dt_data") == 6,  "Junho")
     .when(F.month("dt_data") == 7,  "Julho")
     .when(F.month("dt_data") == 8,  "Agosto")
     .when(F.month("dt_data") == 9,  "Setembro")
     .when(F.month("dt_data") == 10, "Outubro")
     .when(F.month("dt_data") == 11, "Novembro")
     .otherwise("Dezembro")
)

dias_semana_case = (
    F.when(F.dayofweek("dt_data") == 2, "Segunda")
     .when(F.dayofweek("dt_data") == 3, "Terça")
     .when(F.dayofweek("dt_data") == 4, "Quarta")
     .when(F.dayofweek("dt_data") == 5, "Quinta")
     .when(F.dayofweek("dt_data") == 6, "Sexta")
     .when(F.dayofweek("dt_data") == 7, "Sábado")
     .otherwise("Domingo")
)

df_dim_data = (
    df_datas
    .withColumn("sk_data",         (F.year("dt_data") * 10000 + F.month("dt_data") * 100 + F.dayofmonth("dt_data")).cast(IntegerType()))
    .withColumn("nr_ano",          F.year("dt_data").cast(IntegerType()))
    .withColumn("nr_mes",          F.month("dt_data").cast(IntegerType()))
    .withColumn("nr_dia",          F.dayofmonth("dt_data").cast(IntegerType()))
    .withColumn("nm_mes",          meses_case)
    .withColumn("nm_dia_semana",   dias_semana_case)
    .withColumn("nr_trimestre",    F.quarter("dt_data").cast(IntegerType()))
    .withColumn("is_fim_semana",   F.dayofweek("dt_data").isin(1, 7))
    .select("sk_data", "dt_data", "nr_ano", "nr_mes", "nr_dia",
            "nm_mes", "nm_dia_semana", "nr_trimestre", "is_fim_semana")
    .orderBy("dt_data")
)

(
    df_dim_data.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_data")
)

print(f"[OK] dim_data → {df_dim_data.count()} datas")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. DIM_CLIENTE — Dimensão Clientes

# COMMAND ----------

df_dim_cliente = (
    spark.read.table(f"{SILVER_SCHEMA}.cliente")
    .select(
        F.col("cd_cliente").alias("sk_cliente"),
        "cd_cliente",
        "nome",
        "cpf",
        "sexo",
        "dt_nascimento",
        "nr_idade",
        "faixa_etaria",
    )
)

(
    df_dim_cliente.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_cliente")
)

print(f"[OK] dim_cliente → {df_dim_cliente.count()} clientes")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. DIM_VEICULO — Dimensão Veículos (carro + modelo + marca)

# COMMAND ----------

df_carro  = spark.read.table(f"{SILVER_SCHEMA}.carro")
df_modelo = spark.read.table(f"{SILVER_SCHEMA}.modelo")
df_marca  = spark.read.table(f"{SILVER_SCHEMA}.marca")

w = Window.orderBy("placa")

df_dim_veiculo = (
    df_carro
    .join(df_modelo, on="cd_modelo", how="left")
    .join(df_marca,  on="cd_marca",  how="left")
    .select(
        F.row_number().over(w).cast(IntegerType()).alias("sk_veiculo"),
        "placa",
        "chassi",
        F.col("ano").alias("ano_fabricacao"),
        "cor",
        "cd_modelo",
        "nm_modelo",
        "cd_marca",
        "nm_marca",
        "faixa_ano",
    )
)

(
    df_dim_veiculo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_veiculo")
)

print(f"[OK] dim_veiculo → {df_dim_veiculo.count()} veículos")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. DIM_LOCALIZACAO — Dimensão Geográfica (municipio + estado + regiao)

# COMMAND ----------

df_municipio = spark.read.table(f"{SILVER_SCHEMA}.municipio")
df_estado    = spark.read.table(f"{SILVER_SCHEMA}.estado")
df_regiao    = spark.read.table(f"{SILVER_SCHEMA}.regiao")

df_dim_localizacao = (
    df_municipio
    .join(df_estado, on="cd_estado", how="left")
    .join(df_regiao, on="cd_regiao", how="left")
    .select(
        F.col("cd_municipio").alias("sk_localizacao"),
        "cd_municipio",
        "nm_municipio",
        "cd_estado",
        "nm_estado",
        "sigla_uf",
        "cd_regiao",
        "nm_regiao",
    )
)

(
    df_dim_localizacao.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.dim_localizacao")
)

print(f"[OK] dim_localizacao → {df_dim_localizacao.count()} municípios")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. FATO_APOLICE — Fato Apólices de Seguro
# MAGIC
# MAGIC **Grão:** uma linha por apólice.
# MAGIC **Métricas:** vl_cobertura, vl_franquia, dias_vigencia.

# COMMAND ----------

df_apolice   = spark.read.table(f"{SILVER_SCHEMA}.apolice")
df_dim_vei   = spark.read.table(f"{GOLD_SCHEMA}.dim_veiculo").select("sk_veiculo", "placa")
df_dim_data2 = spark.read.table(f"{GOLD_SCHEMA}.dim_data").select("sk_data", "dt_data")

df_fato_apolice = (
    df_apolice
    # SK cliente
    .withColumn("sk_cliente", F.col("cd_cliente").cast(IntegerType()))
    # SK veículo via placa
    .join(df_dim_vei, on="placa", how="left")
    # SK data início
    .join(
        df_dim_data2.withColumnRenamed("sk_data", "sk_data_inicio")
                    .withColumnRenamed("dt_data", "dt_inicio"),
        F.col("dt_inicio_vigencia").cast("date") == F.col("dt_inicio"),
        how="left"
    ).drop("dt_inicio")
    # SK data fim
    .join(
        df_dim_data2.withColumnRenamed("sk_data", "sk_data_fim")
                    .withColumnRenamed("dt_data", "dt_fim"),
        F.col("dt_fim_vigencia").cast("date") == F.col("dt_fim"),
        how="left"
    ).drop("dt_fim")
    .select(
        F.col("cd_apolice").alias("sk_apolice"),
        "cd_apolice",
        "sk_cliente",
        "sk_veiculo",
        "sk_data_inicio",
        "sk_data_fim",
        "vl_cobertura",
        "vl_franquia",
        "dias_vigencia",
    )
)

(
    df_fato_apolice.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fato_apolice")
)

print(f"[OK] fato_apolice → {df_fato_apolice.count()} apólices")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 8. FATO_SINISTRO — Fato Sinistros
# MAGIC
# MAGIC **Grão:** uma linha por sinistro.
# MAGIC **Métricas:** qt_sinistros (= 1 por linha).
# MAGIC **Dimensões degeneradas:** condutor.

# COMMAND ----------

df_sinistro  = spark.read.table(f"{SILVER_SCHEMA}.sinistro")
df_dim_vei2  = spark.read.table(f"{GOLD_SCHEMA}.dim_veiculo").select("sk_veiculo", "placa")
df_dim_data3 = spark.read.table(f"{GOLD_SCHEMA}.dim_data").select("sk_data", "dt_data")

df_fato_sinistro = (
    df_sinistro
    # SK veículo via placa
    .join(df_dim_vei2, on="placa", how="left")
    # SK data sinistro
    .join(
        df_dim_data3.withColumnRenamed("sk_data", "sk_data_sinistro")
                    .withColumnRenamed("dt_data", "dt_sin"),
        F.col("dt_sinistro").cast("date") == F.col("dt_sin"),
        how="left"
    ).drop("dt_sin")
    # SK localização via local_sinistro (= cd_municipio)
    .withColumn("sk_localizacao", F.col("local_sinistro").cast(IntegerType()))
    .withColumn("qt_sinistros", F.lit(1).cast(IntegerType()))
    .select(
        F.col("cd_sinistro").alias("sk_sinistro"),
        "cd_sinistro",
        "sk_veiculo",
        "sk_data_sinistro",
        "sk_localizacao",
        "condutor",
        "qt_sinistros",
    )
)

(
    df_fato_sinistro.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.fato_sinistro")
)

print(f"[OK] fato_sinistro → {df_fato_sinistro.count()} sinistros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 9. Validação do Modelo Dimensional

# COMMAND ----------

print("=" * 60)
print("VALIDAÇÃO — SCHEMA GOLD (Modelo Dimensional)")
print("=" * 60)

objetos = [
    ("dim_data",        "Dimensão"),
    ("dim_cliente",     "Dimensão"),
    ("dim_veiculo",     "Dimensão"),
    ("dim_localizacao", "Dimensão"),
    ("fato_apolice",    "Fato"),
    ("fato_sinistro",   "Fato"),
]

for tabela, tipo in objetos:
    cnt = spark.sql(f"SELECT count(*) as c FROM {GOLD_SCHEMA}.{tabela}").collect()[0]["c"]
    print(f"  [{tipo:9s}] {tabela:<18} → {cnt:>7} registros")

print("=" * 60)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10. Queries analíticas de exemplo

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.1 Total de cobertura por ano e mês

# COMMAND ----------

spark.sql("""
    SELECT
        d.nr_ano,
        d.nm_mes,
        COUNT(fa.sk_apolice)         AS qt_apolices,
        SUM(fa.vl_cobertura)         AS total_cobertura,
        AVG(fa.vl_franquia)          AS media_franquia,
        AVG(fa.dias_vigencia)        AS media_dias_vigencia
    FROM gold.fato_apolice fa
    JOIN gold.dim_data d ON fa.sk_data_inicio = d.sk_data
    GROUP BY d.nr_ano, d.nm_mes, d.nr_mes
    ORDER BY d.nr_ano, d.nr_mes
""").show(20, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.2 Sinistros por região e ano

# COMMAND ----------

spark.sql("""
    SELECT
        l.nm_regiao,
        d.nr_ano,
        COUNT(fs.sk_sinistro)  AS qt_sinistros,
        COUNT(DISTINCT fs.condutor) AS qt_condutores_distintos
    FROM gold.fato_sinistro fs
    JOIN gold.dim_localizacao l ON fs.sk_localizacao = l.sk_localizacao
    JOIN gold.dim_data d ON fs.sk_data_sinistro = d.sk_data
    GROUP BY l.nm_regiao, d.nr_ano
    ORDER BY d.nr_ano, qt_sinistros DESC
""").show(20, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.3 Apólices por faixa etária do cliente

# COMMAND ----------

spark.sql("""
    SELECT
        c.faixa_etaria,
        c.sexo,
        COUNT(fa.sk_apolice)   AS qt_apolices,
        AVG(fa.vl_cobertura)   AS media_cobertura,
        SUM(fa.vl_franquia)    AS total_franquia
    FROM gold.fato_apolice fa
    JOIN gold.dim_cliente c ON fa.sk_cliente = c.sk_cliente
    GROUP BY c.faixa_etaria, c.sexo
    ORDER BY c.faixa_etaria, c.sexo
""").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ### 10.4 Top 10 marcas de veículos com mais sinistros

# COMMAND ----------

spark.sql("""
    SELECT
        v.nm_marca,
        v.faixa_ano,
        COUNT(fs.sk_sinistro) AS qt_sinistros
    FROM gold.fato_sinistro fs
    JOIN gold.dim_veiculo v ON fs.sk_veiculo = v.sk_veiculo
    GROUP BY v.nm_marca, v.faixa_ano
    ORDER BY qt_sinistros DESC
    LIMIT 10
""").show(truncate=False)
