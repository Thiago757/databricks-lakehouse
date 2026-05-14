# Guia de Configuração

## 1. Supabase (banco de dados fonte)

### 1.1 Criar projeto

1. Acesse [supabase.com](https://supabase.com) → **Sign Up** (gratuito)
2. Clique em **New Project**
3. Preencha:
   - **Name:** seguros-db
   - **Database Password:** (anote bem essa senha!)
   - **Region:** South America (São Paulo)
4. Aguarde a criação (~60 segundos)

### 1.2 Obter credenciais

Vá em **Project Settings → Database** e anote:

| Campo | Exemplo |
|---|---|
| **Host** | `db.abcxyzabc.supabase.co` |
| **Port** | `5432` |
| **Database** | `postgres` |
| **User** | `postgres` |
| **Password** | (a que você definiu) |

### 1.3 Criar as tabelas (DDL)

1. No painel do Supabase, vá em **SQL Editor**
2. Clique em **New query**
3. Cole o conteúdo do arquivo `sql/supabase_ddl.sql`
4. Clique em **Run** (▶)

### 1.4 Carregar os dados (CSVs)

Certifique-se de que os CSVs estão na pasta `data/` do projeto.

```bash
# Instalar dependências
pip install psycopg2-binary pandas

# Editar o script com suas credenciais
# Abra sql/supabase_load_data.py e preencha:
#   SUPABASE_HOST     = "db.<ref>.supabase.co"
#   SUPABASE_PASSWORD = "<sua_senha>"

# Executar
python sql/supabase_load_data.py
```

Saída esperada:
```
=== Carregando dados no Supabase ===

Conectado em: db.xxx.supabase.co/postgres

  [OK] regiao       →      5 registros inseridos
  [OK] estado       →     27 registros inseridos
  [OK] municipio    →   5570 registros inseridos
  ...
  [OK] sinistro     →  10000 registros inseridos

Carga concluída com sucesso!
```

---

## 2. Databricks Free Edition

### 2.1 Criar conta

1. Acesse [community.cloud.databricks.com](https://community.cloud.databricks.com)
2. Clique em **Get started for free**
3. Preencha o formulário e confirme o e-mail

### 2.2 Criar cluster

1. No menu lateral, clique em **Compute**
2. Clique em **Create compute** (ou **Create cluster**)
3. Configure:
   - **Cluster name:** lakehouse-trabalho3
   - **Runtime:** `15.x LTS (Scala 2.12, Spark 3.5.x)` ← Delta já incluso
   - **Terminate after:** 30 minutes of inactivity
4. Clique em **Create compute**

### 2.3 Importar notebooks

1. No menu lateral, clique em **Workspace**
2. Clique com o botão direito em uma pasta → **Import**
3. Selecione **File** e faça upload de cada `.py` da pasta `notebooks/`:
   - `01_landing.py`
   - `02_bronze.py`
   - `03_silver.py`
   - `04_gold.py`

> **Dica:** O Databricks reconhece automaticamente os arquivos `.py` com o header
> `# Databricks notebook source` como notebooks nativos.

### 2.4 Configurar credenciais (Notebook 01)

Abra o notebook `01_landing` e preencha os **widgets** na parte superior:

| Widget | Valor |
|---|---|
| `SUPABASE_HOST` | `db.<ref>.supabase.co` |
| `SUPABASE_DB` | `postgres` |
| `SUPABASE_USER` | `postgres` |
| `SUPABASE_PASSWORD` | `<sua_senha>` |
| `SUPABASE_PORT` | `5432` |

---

## 3. Criar e executar o Job

### 3.1 Criar o Job

1. No menu lateral, clique em **Workflows**
2. Clique em **Create Job**
3. Dê o nome: **Pipeline Lakehouse — Trabalho 3**

### 3.2 Adicionar as 4 tasks

Para cada notebook, adicione uma task:

**Task 1 — Landing:**
- Task name: `01_landing`
- Type: `Notebook`
- Source: `Workspace` → selecione `01_landing`
- Cluster: o que você criou

**Task 2 — Bronze:**
- Task name: `02_bronze`
- Type: `Notebook`
- Source: `Workspace` → selecione `02_bronze`
- Depends on: `01_landing`

**Task 3 — Silver:**
- Task name: `03_silver`
- Type: `Notebook`
- Source: `Workspace` → selecione `03_silver`
- Depends on: `02_bronze`

**Task 4 — Gold:**
- Task name: `04_gold`
- Type: `Notebook`
- Source: `Workspace` → selecione `04_gold`
- Depends on: `03_silver`

### 3.3 Executar

Clique em **Run now** e acompanhe o progresso de cada task.

---

## 4. Verificar os resultados

Após o Job concluir (todas as tasks em verde), verifique no **SQL Editor** do Databricks:

```sql
-- Contagens por schema
SELECT 'landing'  AS schema, 'apolice' AS tabela, count(*) AS registros FROM landing.apolice
UNION ALL
SELECT 'bronze',  'apolice', count(*) FROM bronze.apolice
UNION ALL
SELECT 'silver',  'apolice', count(*) FROM silver.apolice
UNION ALL
SELECT 'gold',    'fato_apolice', count(*) FROM gold.fato_apolice;

-- Modelo dimensional funcionando
SELECT c.faixa_etaria, COUNT(*) AS qt_apolices
FROM gold.fato_apolice fa
JOIN gold.dim_cliente c ON fa.sk_cliente = c.sk_cliente
GROUP BY c.faixa_etaria
ORDER BY qt_apolices DESC;
```
