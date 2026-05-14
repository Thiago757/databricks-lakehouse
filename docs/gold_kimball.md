# Modelo Dimensional — Ralph Kimball (Gold)

## O que é o Modelo Kimball?

A metodologia **Ralph Kimball** propõe organizar os dados em um **modelo dimensional**
(star schema / snowflake), separando os dados em:

- **Dimensões:** tabelas que descrevem o contexto de um evento (quem, onde, quando, o quê)
- **Fatos:** tabelas que registram eventos de negócio com métricas numéricas

### Vantagens
- Consultas analíticas mais simples e rápidas
- Estrutura intuitiva para analistas de negócio
- Compatível com ferramentas de BI (Power BI, Tableau, Metabase)

---

## Star Schema do SeguroDB

```
                    ┌──────────────┐
                    │  dim_data    │
                    │  (Calendário)│
                    └──────┬───────┘
                           │ sk_data_inicio / sk_data_fim
          ┌────────────────┤
          │                ▼
┌─────────┴──────┐   ┌─────────────────┐   ┌────────────────┐
│  dim_cliente   │   │  fato_apolice   │   │  dim_veiculo   │
│  (Clientes)    ├───┤  (por apólice)  ├───┤  (Veículos)    │
└────────────────┘   └─────────────────┘   └────────────────┘


                    ┌──────────────┐
                    │  dim_data    │
                    └──────┬───────┘
                           │ sk_data_sinistro
          ┌────────────────┤
          │                ▼
┌─────────┴──────┐   ┌─────────────────┐   ┌────────────────────┐
│  dim_veiculo   │   │  fato_sinistro  │   │  dim_localizacao   │
│  (Veículos)    ├───┤  (por sinistro) ├───┤  (Geografia)       │
└────────────────┘   └─────────────────┘   └────────────────────┘
```

---

## Dimensões

### dim_data — Calendário

Gerada automaticamente a partir das datas presentes nos fatos.
`sk_data` é um inteiro no formato **YYYYMMDD** (ex: 20240315).

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_data` | INT (PK) | Chave surrogate: YYYYMMDD |
| `dt_data` | DATE | Data completa |
| `nr_ano` | INT | Ano (ex: 2024) |
| `nr_mes` | INT | Mês numérico (1-12) |
| `nr_dia` | INT | Dia do mês (1-31) |
| `nm_mes` | STRING | Nome do mês (Janeiro, Fevereiro...) |
| `nm_dia_semana` | STRING | Dia da semana (Segunda, Terça...) |
| `nr_trimestre` | INT | Trimestre (1-4) |
| `is_fim_semana` | BOOLEAN | True se sábado ou domingo |

---

### dim_cliente — Clientes

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_cliente` | INT (PK) | Chave surrogate (= cd_cliente) |
| `cd_cliente` | INT | Código original |
| `nome` | STRING | Nome completo |
| `cpf` | STRING | CPF (11 dígitos) |
| `sexo` | STRING | M ou F |
| `dt_nascimento` | DATE | Data de nascimento |
| `nr_idade` | INT | Idade em anos (calculada) |
| `faixa_etaria` | STRING | Menor de 18 / 18-30 / 31-45 / 46-60 / Acima de 60 |

---

### dim_veiculo — Veículos

Join de `carro` + `modelo` + `marca` da camada Silver.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_veiculo` | INT (PK) | Chave surrogate (row_number) |
| `placa` | STRING | Placa do veículo |
| `chassi` | STRING | Número do chassi |
| `ano_fabricacao` | INT | Ano de fabricação |
| `cor` | STRING | Cor do veículo |
| `cd_modelo` | INT | Código do modelo |
| `nm_modelo` | STRING | Nome do modelo |
| `cd_marca` | INT | Código da marca |
| `nm_marca` | STRING | Nome da marca |
| `faixa_ano` | STRING | Antes de 2000 / 2000-2010 / 2011-2020 / 2021 em diante |

---

### dim_localizacao — Geografia

Join de `municipio` + `estado` + `regiao` da camada Silver.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_localizacao` | INT (PK) | Chave surrogate (= cd_municipio) |
| `cd_municipio` | INT | Código do município (IBGE) |
| `nm_municipio` | STRING | Nome do município |
| `cd_estado` | INT | Código do estado |
| `nm_estado` | STRING | Nome do estado |
| `sigla_uf` | STRING | Sigla UF (ex: SP, RJ) |
| `cd_regiao` | INT | Código da região |
| `nm_regiao` | STRING | Nome da região (Norte, Sul...) |

---

## Fatos

### fato_apolice — Apólices de Seguro

**Grão:** uma linha por apólice de seguro.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_apolice` | INT (PK) | Chave surrogate (= cd_apolice) |
| `cd_apolice` | INT | Código da apólice (dim degenerada) |
| `sk_cliente` | INT (FK) | Referência à dim_cliente |
| `sk_veiculo` | INT (FK) | Referência à dim_veiculo |
| `sk_data_inicio` | INT (FK) | Referência à dim_data (início da vigência) |
| `sk_data_fim` | INT (FK) | Referência à dim_data (fim da vigência) |
| `vl_cobertura` | DECIMAL(15,2) | Valor de cobertura (R$) |
| `vl_franquia` | DECIMAL(15,2) | Valor da franquia (R$) |
| `dias_vigencia` | INT | Duração da apólice em dias |

---

### fato_sinistro — Sinistros Registrados

**Grão:** uma linha por sinistro.

| Coluna | Tipo | Descrição |
|---|---|---|
| `sk_sinistro` | INT (PK) | Chave surrogate (= cd_sinistro) |
| `cd_sinistro` | INT | Código do sinistro (dim degenerada) |
| `sk_veiculo` | INT (FK) | Referência à dim_veiculo |
| `sk_data_sinistro` | INT (FK) | Referência à dim_data |
| `sk_localizacao` | INT (FK) | Referência à dim_localizacao |
| `condutor` | STRING | Nome do condutor (dim degenerada) |
| `qt_sinistros` | INT | Quantidade (sempre = 1, para soma) |

---

## Exemplos de queries analíticas

### Total de cobertura por mês e ano
```sql
SELECT
    d.nr_ano,
    d.nm_mes,
    COUNT(fa.sk_apolice)   AS qt_apolices,
    SUM(fa.vl_cobertura)   AS total_cobertura,
    AVG(fa.vl_franquia)    AS media_franquia
FROM gold.fato_apolice fa
JOIN gold.dim_data d ON fa.sk_data_inicio = d.sk_data
GROUP BY d.nr_ano, d.nm_mes, d.nr_mes
ORDER BY d.nr_ano, d.nr_mes;
```

### Sinistros por região e ano
```sql
SELECT
    l.nm_regiao,
    d.nr_ano,
    SUM(fs.qt_sinistros) AS total_sinistros
FROM gold.fato_sinistro fs
JOIN gold.dim_localizacao l ON fs.sk_localizacao = l.sk_localizacao
JOIN gold.dim_data d ON fs.sk_data_sinistro = d.sk_data
GROUP BY l.nm_regiao, d.nr_ano
ORDER BY d.nr_ano, total_sinistros DESC;
```

### Apólices por perfil de cliente
```sql
SELECT
    c.faixa_etaria,
    c.sexo,
    COUNT(fa.sk_apolice)   AS qt_apolices,
    AVG(fa.vl_cobertura)   AS media_cobertura
FROM gold.fato_apolice fa
JOIN gold.dim_cliente c ON fa.sk_cliente = c.sk_cliente
GROUP BY c.faixa_etaria, c.sexo
ORDER BY c.faixa_etaria;
```

### Top marcas com mais sinistros
```sql
SELECT
    v.nm_marca,
    COUNT(fs.sk_sinistro) AS qt_sinistros
FROM gold.fato_sinistro fs
JOIN gold.dim_veiculo v ON fs.sk_veiculo = v.sk_veiculo
GROUP BY v.nm_marca
ORDER BY qt_sinistros DESC
LIMIT 10;
```
