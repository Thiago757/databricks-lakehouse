# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Notebook 01 — Extração: Supabase → LANDING
# MAGIC
# MAGIC Extrai todas as tabelas do banco **SeguroDB** no **Supabase (PostgreSQL)** via
# MAGIC **REST API (HTTPS)** e grava como **Delta managed tables** no schema `landing`.
# MAGIC
# MAGIC **Fluxo:**
# MAGIC ```
# MAGIC Supabase PostgreSQL (11 tabelas)
# MAGIC     ── REST API HTTPS ──► requests
# MAGIC     ── pandas ──► Spark DataFrame
# MAGIC     ── Delta Write ──► landing.{tabela}
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import requests
import pandas as pd

SUPABASE_URL = "https://ctcmfwdlrrxntpkhstzb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImN0Y21md2RscnJ4bnRwa2hzdHpiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3ODY4MTYyMiwiZXhwIjoyMDk0MjU3NjIyfQ.tJRG-qBzLT110vtRQchRc5_WMtwpJU_5-ogt3Uy2Rpo"

HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

LANDING_SCHEMA = "landing"

TABELAS = [
    "regiao", "estado", "municipio", "marca", "modelo",
    "cliente", "endereco", "telefone", "carro", "apolice", "sinistro",
]

print(f"Supabase : {SUPABASE_URL}")
print(f"Schema   : {LANDING_SCHEMA}")
print(f"Tabelas  : {len(TABELAS)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Criar schema LANDING

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {LANDING_SCHEMA}")
print(f"Schema '{LANDING_SCHEMA}' pronto.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Extrair tabelas do Supabase e gravar no LANDING

# COMMAND ----------

def fetch_tabela(tabela: str) -> list:
    """Extrai todos os registros via REST API com paginação de 1000 linhas."""
    dados, offset, page_size = [], 0, 1000
    while True:
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/{tabela}",
            headers={**HEADERS, "Range": f"{offset}-{offset + page_size - 1}"},
            params={"select": "*"},
        )
        resp.raise_for_status()
        pagina = resp.json()
        if not pagina:
            break
        dados.extend(pagina)
        if len(pagina) < page_size:
            break
        offset += page_size
    return dados

resultados = []

for tabela in TABELAS:
    dados = fetch_tabela(tabela)

    (
        spark.createDataFrame(pd.DataFrame(dados))
             .write
             .format("delta")
             .mode("overwrite")
             .option("overwriteSchema", "true")
             .saveAsTable(f"{LANDING_SCHEMA}.{tabela}")
    )

    resultados.append({"tabela": tabela, "registros": len(dados)})
    print(f"  [OK] {tabela:12s} → {len(dados):>6} registros → {LANDING_SCHEMA}.{tabela}")

print(f"\nTotal: {sum(r['registros'] for r in resultados)} registros extraídos")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Validação

# COMMAND ----------

print("=== Validação do schema LANDING ===\n")
for r in resultados:
    count = spark.sql(f"SELECT count(*) as c FROM {LANDING_SCHEMA}.{r['tabela']}").collect()[0]["c"]
    status = "OK" if count == r["registros"] else "DIVERGÊNCIA"
    print(f"  [{status}] {r['tabela']:12s} | Supabase={r['registros']:>6} | Landing={count:>6}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Amostra dos dados

# COMMAND ----------

spark.sql("SELECT * FROM landing.apolice LIMIT 5").show(truncate=False)
spark.sql("SELECT * FROM landing.cliente LIMIT 5").show(truncate=False)
