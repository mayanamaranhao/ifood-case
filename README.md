# Case Técnico Data Architect — iFood

Solução de ingestão, modelagem e disponibilização dos dados de corridas de
yellow táxi de NY (Jan–Mai/2023), usando **arquitetura medalhão**
(Bronze / Silver / Gold) em PySpark + Delta Lake, pensada para rodar em
Databricks (Community Edition ou não).

## Arquitetura

```
NYC TLC (parquet) → Landing Zone (UC Volume) → Bronze → Silver → Gold → Consumo (SQL)
```

> **Nota sobre o ambiente**: a antiga Databricks Community Edition foi
> aposentada em 01/01/2026 e substituída pela **Databricks Free Edition**.
> Diferenças relevantes para este projeto: não há mais clusters
> customizados (só **serverless compute**), não há mais DBFS root, e o
> **Unity Catalog é obrigatório** — por isso a landing zone usa um
> **Volume** do Unity Catalog, e as tabelas Bronze/Silver/Gold são
> schemas dentro do catalog (`workspace.bronze`, `workspace.silver`,
> `workspace.gold`) em vez do antigo Hive Metastore `database`.

| Camada  | Conteúdo | Formato | Onde vive | Propósito |
|---------|----------|---------|-----------|-----------|
| **Landing** | Arquivos originais, intocados | Parquet (como publicado) | UC Volume: `/Volumes/workspace/default/ifood_case/landing/yellow_taxi` | reprocessamento sem depender da fonte externa |
| **Bronze** | Raw + metadados técnicos (`_ingested_at`, `_source_file`, `_ref_year`, `_ref_month`) | Delta (tabela gerenciada) | `workspace.bronze.yellow_taxi_trips` | histórico bruto, schema-on-read tolerante |
| **Silver** | Tipado, limpo, deduplicado, apenas colunas exigidas + derivadas | Delta (tabela gerenciada) | `workspace.silver.yellow_taxi_trips` | fonte única de verdade para análises |
| **Gold** | Agregados de negócio prontos para consumo | Delta (tabela gerenciada) | `workspace.gold.*` | resposta direta às perguntas de negócio |

### Por que medalhão?
- **Bronze imutável** garante que qualquer erro de regra de limpeza na
  Silver seja corrigível via reprocessamento, sem re-baixar da fonte.
- **Silver como contrato único** evita que cada analista reimplemente as
  mesmas regras de qualidade (passenger_count > 0, total_amount >= 0,
  dropoff >= pickup) em queries diferentes.
- **Gold pré-agregada** entrega as respostas de negócio como tabelas
  simples de `SELECT`, sem exigir que o usuário final saiba fazer os
  agrupamentos corretamente.

### Decisões técnicas
- **Delta Lake** em vez de Parquet puro nas camadas Bronze/Silver/Gold:
  ACID, schema evolution controlada (`overwriteSchema`), e suporte nativo
  a `MERGE`/upsert para evoluções futuras (ex.: ingestão incremental).
- **Tabelas gerenciadas pelo Unity Catalog** (`saveAsTable` sem `path`
  explícito): a Free Edition não permite apontar para locations externas
  arbitrárias sem um External Location configurado, então o storage físico
  fica a cargo do UC.
- **Linguagem de consulta**: SQL sobre as tabelas Gold/Silver via Spark
  SQL — qualquer usuário com acesso ao workspace consulta sem precisar
  escrever PySpark.
- **Qualidade de dados**: filtros documentados no código
  (`filter_invalid_records`), aplicados uma única vez na Silver, não
  espalhados pelas análises.
- **Particionamento por data de negócio (`trip_year`/`trip_month`)** a
  partir da Silver, e não pelo nome do arquivo, pois o TLC ocasionalmente
  publica corridas de meses vizinhos dentro do arquivo "errado".

## Estrutura do repositório

```
ifood-case/
├─ src/
│  ├─ config.py                        # paths, databases, parâmetros de ingestão
│  ├─ common/
│  │  ├─ spark_session.py              # factory de SparkSession (Delta configurado)
│  │  ├─ schemas.py                    # schema explícito da Silver
│  │  └─ utils.py                      # logger, ensure_database
│  ├─ ingestion/
│  │  ├─ download_taxi_data.py         # NYC TLC → landing zone (idempotente)
│  │  └─ landing_to_bronze.py          # landing → Bronze (Delta)
│  └─ transformation/
│     ├─ bronze_to_silver.py           # limpeza, tipagem, dedup, qualidade
│     └─ silver_to_gold.py             # agregados de negócio (as 2 perguntas)
├─ analysis/
│  ├─ 01_avg_total_amount_by_month.sql
│  ├─ 02_avg_passenger_count_by_hour_may.sql
│  └─ 03_exploratory_analysis.py
├─ tests/
│  └─ test_transformations.py          # pytest, roda com Spark local (sem cluster)
├─ README.md
└─ requirements.txt
```

## Como executar (Databricks Free Edition)

1. Suba o repositório via **Databricks Repos** (integração direta com Git) — fica em `/Workspace/Repos/<usuário>/ifood-case`.
2. Crie o **Volume** que serve de landing zone: **Catalog** → catalog `workspace` → schema `default` → **Create** → **Volume** → nome `ifood_case`.
   Confira se o path bate com `LAKE.landing_path` em `src/config.py` (ajuste `catalog`/`volume_schema`/`volume_name` se necessário).
3. Abra um notebook dentro do Repo — o compute **serverless** já vem selecionado por padrão (não é preciso criar cluster).
4. Execute em sequência:
   ```python
   from src.ingestion import download_taxi_data, landing_to_bronze
   from src.transformation import bronze_to_silver, silver_to_gold

   download_taxi_data.run()      # NYC TLC -> landing zone (Volume)
   landing_to_bronze.run()       # landing -> bronze (Delta, tabela gerenciada UC)
   bronze_to_silver.run()        # bronze -> silver (Delta)
   silver_to_gold.run()          # silver -> gold (Delta)
   ```
   Se `download_taxi_data.run()` falhar por restrição de rede da Free
   Edition (domínio não liberado), baixe os 5 arquivos manualmente e
   suba via **Catalog > Volume > Upload to this volume** no mesmo path
   de `LAKE.landing_path`, depois rode a partir de `landing_to_bronze`.
5. Consulte as respostas via SQL (`analysis/*.sql`) no **SQL Editor** do
   Databricks ou via `spark.sql(...)` no notebook.

## Como rodar os testes localmente

```bash
pip install -r requirements.txt
pytest tests/ -v
```

## Perguntas de negócio respondidas

1. **Média de `total_amount` por mês (toda a frota)** →
   `gold.avg_total_amount_by_month` / `analysis/01_avg_total_amount_by_month.sql`
2. **Média de `passenger_count` por hora do dia em maio/2023** →
   `gold.avg_passenger_count_by_hour_may` / `analysis/02_avg_passenger_count_by_hour_may.sql`

## Possíveis evoluções (fora do escopo mínimo do case)
- Ingestão incremental (Auto Loader / `MERGE` em vez de `overwrite`).
- Orquestração via Airflow/Databricks Workflows com retries e alertas.
- Testes de qualidade automatizados (Great Expectations / Delta
  constraints) rodando como gate entre camadas.
- Unity Catalog para governança/linhagem entre Bronze/Silver/Gold.
