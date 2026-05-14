# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 02 — Ingestão: LANDING → BRONZE (Delta Lake)
# MAGIC
# MAGIC Lê os arquivos **CSV** do schema `landing` e grava cada tabela como
# MAGIC **Delta Lake managed table** no schema `bronze`.
# MAGIC
# MAGIC **Fluxo:**
# MAGIC ```
# MAGIC landing.{tabela}  (CSV / External Table)
# MAGIC     ──Spark Read──► DataFrame
# MAGIC     ──Delta Write──► bronze.{tabela}  (Managed Delta Table)
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

LANDING_SCHEMA = "landing"
BRONZE_SCHEMA  = "bronze"

TABELAS = [
    "regiao",
    "estado",
    "municipio",
    "marca",
    "modelo",
    "cliente",
    "endereco",
    "telefone",
    "carro",
    "apolice",
    "sinistro",
]

print(f"Origem : {LANDING_SCHEMA}")
print(f"Destino: {BRONZE_SCHEMA} (Delta Lake)")
print(f"Tabelas: {len(TABELAS)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Criar schema BRONZE

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {BRONZE_SCHEMA}")
print(f"Schema '{BRONZE_SCHEMA}' pronto.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Converter CSV → Delta Lake

# COMMAND ----------

resultados = []

for tabela in TABELAS:
    # Lê do schema LANDING (external table CSV)
    df = spark.read.table(f"{LANDING_SCHEMA}.{tabela}")
    count = df.count()

    # Grava como Delta managed table no schema BRONZE
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(f"{BRONZE_SCHEMA}.{tabela}")
    )

    resultados.append({"tabela": tabela, "registros": count})
    print(f"  [OK] {tabela:12s} → {count:>6} registros → {BRONZE_SCHEMA}.{tabela} (Delta)")

total = sum(r["registros"] for r in resultados)
print(f"\nTotal: {total} registros convertidos para Delta Lake")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Validação — schema, contagem e Delta

# COMMAND ----------

print("=== Validação das tabelas Delta no schema BRONZE ===\n")
for r in resultados:
    tabela = r["tabela"]
    count_bronze = spark.sql(f"SELECT count(*) as c FROM {BRONZE_SCHEMA}.{tabela}").collect()[0]["c"]
    status = "OK" if count_bronze == r["registros"] else "ERRO"
    print(f"  [{status}] {tabela:12s} | {count_bronze} registros")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Inspecionar histórico Delta (Transaction Log)

# COMMAND ----------

print("=== DESCRIBE HISTORY bronze.apolice ===")
spark.sql("DESCRIBE HISTORY bronze.apolice").show(5, truncate=False)

print("=== DESCRIBE HISTORY bronze.cliente ===")
spark.sql("DESCRIBE HISTORY bronze.cliente").show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 6. Inspecionar schemas das tabelas Bronze

# COMMAND ----------

for tabela in ["apolice", "sinistro", "cliente", "carro"]:
    print(f"\n--- Schema: bronze.{tabela} ---")
    spark.sql(f"DESCRIBE bronze.{tabela}").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 7. Amostra de dados

# COMMAND ----------

print("=== Amostra: bronze.apolice ===")
spark.sql("SELECT * FROM bronze.apolice LIMIT 5").show(truncate=False)

print("=== Amostra: bronze.sinistro ===")
spark.sql("SELECT * FROM bronze.sinistro LIMIT 5").show(truncate=False)
