"""
Trabalho 3 — Script de carga dos CSVs no Supabase (PostgreSQL)

Pré-requisitos:
    pip install psycopg2-binary pandas

Configuração:
    Edite as variáveis SUPABASE_HOST e SUPABASE_PASSWORD abaixo com
    as credenciais do seu projeto Supabase.

Execução:
    python supabase_load_data.py
"""

import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# =============================================================================
# CONFIGURAÇÃO — edite aqui com suas credenciais do Supabase
# =============================================================================
SUPABASE_HOST     = "db.ctcmfwdlrrxntpkhstzb.supabase.co"   # ex: db.abcxyz.supabase.co
SUPABASE_DB       = "postgres"
SUPABASE_USER     = "postgres"
SUPABASE_PASSWORD = "Guilherme34474462g*"
SUPABASE_PORT     = 5432

# Caminho para a pasta com os CSVs (ajuste se necessário)
CSV_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

# =============================================================================
# Ordem de carga respeita as FKs
# =============================================================================
TABELAS_ORDEM = [
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

# Mapeamento: tabela → lista de colunas (mesma ordem do CSV)
COLUNAS = {
    "regiao":    ["cd_regiao", "nm_regiao"],
    "estado":    ["cd_estado", "cd_regiao", "nm_estado", "sigla_uf"],
    "municipio": ["cd_municipio", "nm_municipio", "cd_estado"],
    "marca":     ["cd_marca", "nm_marca"],
    "modelo":    ["cd_modelo", "cd_marca", "nm_modelo"],
    "cliente":   ["cd_cliente", "nome", "cpf", "sexo", "dt_nascimento"],
    "endereco":  ["cd_cliente", "cd_municipio", "ds_endereco", "nr_endereco", "bairro"],
    "telefone":  ["cd_cliente", "nr_telefone"],
    "carro":     ["placa", "cd_modelo", "chassi", "ano", "cor"],
    "apolice":   ["cd_apolice", "cd_cliente", "dt_inicio_vigencia", "dt_fim_vigencia",
                  "vl_cobertura", "vl_franquia", "placa"],
    "sinistro":  ["cd_sinistro", "placa", "dt_sinistro", "local_sinistro", "condutor"],
}


def conectar():
    return psycopg2.connect(
        host=SUPABASE_HOST,
        port=SUPABASE_PORT,
        dbname=SUPABASE_DB,
        user=SUPABASE_USER,
        password=SUPABASE_PASSWORD,
        sslmode="require",
    )


def carregar_tabela(conn, tabela: str):
    csv_path = os.path.join(CSV_DIR, f"{tabela}.csv")
    if not os.path.exists(csv_path):
        print(f"  [AVISO] CSV não encontrado: {csv_path}")
        return

    df = pd.read_csv(csv_path)

    # Trim em colunas de string (placa tem espaços no CSV original)
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()

    cols = COLUNAS[tabela]
    df = df[cols]

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    col_str = ", ".join(cols)
    placeholders = "(" + ", ".join(["%s"] * len(cols)) + ")"

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {tabela} RESTART IDENTITY CASCADE")
        execute_values(
            cur,
            f"INSERT INTO {tabela} ({col_str}) VALUES %s",
            rows,
            template=placeholders,
            page_size=500,
        )

    conn.commit()
    print(f"  [OK] {tabela:12s} → {len(rows):>6} registros inseridos")


def main():
    print("=== Carregando dados no Supabase ===\n")
    conn = conectar()
    print(f"Conectado em: {SUPABASE_HOST}/{SUPABASE_DB}\n")

    try:
        for tabela in TABELAS_ORDEM:
            carregar_tabela(conn, tabela)
    finally:
        conn.close()

    print("\nCarga concluída com sucesso!")


if __name__ == "__main__":
    main()
