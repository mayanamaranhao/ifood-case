# Case Técnico Data Architect - iFood

## 📋 Sobre o Projeto

Este repositório contém a solução completa para o **Case Técnico Data Architect do iFood**, demonstrando competências em:

- **Engenharia de Dados**: Pipeline de ingestão completo usando arquitetura medalhão (Bronze → Silver → Gold)
- **PySpark**: Processamento distribuído de grandes volumes de dados
- **Databricks**: Ambiente moderno de analytics com Unity Catalog, Delta Lake e Serverless Compute
- **Análise de Dados**: Respostas às questões de negócio com SQL e visualizações

O projeto implementa a ingestão e análise de **15 milhões de corridas de Yellow Taxi de NYC** (janeiro a maio de 2023), totalizando ~700 MB de dados em formato Parquet.

---

## 🎯 Objetivos do Case

Conforme especificado no case técnico:

1. ✅ **Ingestão de dados** das corridas de táxi de NYC no Data Lake
2. ✅ **Disponibilização** dos dados para consumidores via SQL
3. ✅ **Análises** respondendo às questões de negócio:
   - Qual a média de valor total (`total_amount`) por mês?
   - Qual a média de passageiros (`passenger_count`) por hora do dia em maio?

---

## 🗂️ Estrutura do Repositório

```
ifood-case/
├── README.md                          # Este arquivo
├── requirements.txt                   # Dependências do projeto
├── .gitignore                         # Arquivos ignorados pelo Git
├── src/                               # Código fonte estruturado
│   ├── bronze/                        # Transformações camada Bronze
│   ├── silver/                        # Transformações camada Silver
│   ├── gold/                          # Transformações camada Gold
│   └── config.py                      # Configurações
├── notebooks/                         # Pipeline de ingestão (PySpark)
│   ├── 00_download_landing.ipynb      # Download dos dados NYC TLC
│   ├── 01_landing_to_bronze.ipynb     # Ingestão Bronze (dados brutos)
│   ├── 02_bronze_to_silver.ipynb      # Transformação Silver (limpeza)
│   └── 03_silver_to_gold.ipynb        # Agregações Gold (negócio)
├── analysis/                          # Respostas às questões do case
│   ├── 01_avg_total_amount_by_month.sql         # Análise 1 (média mensal)
│   ├── 02_avg_passenger_count_by_hour_may.sql   # Análise 2 (média por hora)
│   └── 03_exploratory_analysis.py               # Análise exploratória
└── tests/                             # Testes unitários
```

---

## 🏗️ Arquitetura da Solução

### Arquitetura Medalhão (Bronze → Silver → Gold)

```
NYC TLC Parquet Files
        ↓
Landing Zone (UC Volume: /Volumes/workspace/default/ifood_case/)
        ↓
Bronze Layer (workspace.bronze.yellow_taxi_trips)
  • Dados brutos + metadados de controle
  • Schema original preservado
        ↓
Silver Layer (workspace.silver.yellow_taxi_trips)
  • Dados limpos e validados
  • Colunas obrigatórias garantidas: VendorID, passenger_count, 
    total_amount, tpep_pickup_datetime, tpep_dropoff_datetime
  • Deduplicação e tratamento de nulos
        ↓
Gold Layer (workspace.gold.*)
  • Agregações de negócio
  • Tables: avg_total_amount_by_month, avg_passenger_count_by_hour_may
  • Otimizadas para consulta
        ↓
Consumo (SQL / Genie AI/BI)
```

### Componentes Databricks

**Unity Catalog**
- **Catalog**: `workspace` (container de alto nível)
- **Schemas**: `bronze`, `silver`, `gold` (organização em camadas)
- **Volume**: `ifood_case` (Landing Zone para arquivos parquet)
- **Tables**: Delta Lake com transações ACID

**Compute: Databricks Serverless**
- **Por quê?** Única opção disponível na Community Edition (não permite criar clusters customizados)
- **Vantagens**: Zero configuração, start rápido (~10s), escala automática, custo pay-per-second
- **Adequação**: Dataset de ~3 GB é ideal para Serverless

**Genie AI/BI**
- Interface conversacional para consultar dados em linguagem natural
- Space configurado: "Yellow Taxi - Análise de Corridas"
- Democratiza acesso aos dados (usuários não-técnicos podem explorar via perguntas)

---

## 🚀 Como Executar

### Pré-requisitos

- **Databricks Workspace** (Community Edition ou superior)
- **Compute**: Serverless (automático na Community Edition)
- **Unity Catalog**: Habilitado

### Passo 1: Setup Inicial

Clone ou importe este repositório no Databricks Workspace usando **Git Folders**.

### Passo 2: Executar Pipeline de Ingestão

Execute os notebooks na pasta `notebooks/` **em ordem**:

1. **00_download_landing.ipynb**
   - Faz download dos arquivos parquet da NYC TLC (Jan-Mai 2023)
   - Salva na Landing Zone (`/Volumes/workspace/default/ifood_case/landing/`)
   - Tempo: ~5-10 minutos

2. **01_landing_to_bronze.ipynb**
   - Lê arquivos parquet da Landing Zone
   - Cria tabela `workspace.bronze.yellow_taxi_trips` (Delta Lake)
   - Adiciona metadados de controle (`ingestion_timestamp`, `source_file`)
   - Tempo: ~2-3 minutos

3. **02_bronze_to_silver.ipynb**
   - Limpa e valida dados da camada Bronze
   - Garante colunas obrigatórias: `VendorID`, `passenger_count`, `total_amount`, 
     `tpep_pickup_datetime`, `tpep_dropoff_datetime`
   - Remove duplicatas e valores inválidos
   - Cria tabela `workspace.silver.yellow_taxi_trips`
   - Tempo: ~3-5 minutos

4. **03_silver_to_gold.ipynb**
   - Cria agregações de negócio a partir da camada Silver
   - Gera tabelas Gold:
     - `workspace.gold.avg_total_amount_by_month` (resposta à Pergunta 1)
     - `workspace.gold.avg_passenger_count_by_hour_may` (resposta à Pergunta 2)
   - Tempo: ~1-2 minutos

### Passo 3: Consultar Resultados

**Opção A: SQL**

Execute as queries em `analysis/`:

```sql
-- Pergunta 1: Média de valor total por mês
SELECT * FROM workspace.gold.avg_total_amount_by_month
ORDER BY trip_month;

-- Pergunta 2: Média de passageiros por hora em maio
SELECT * FROM workspace.gold.avg_passenger_count_by_hour_may
ORDER BY trip_hour;
```

**Opção B: Genie AI/BI**

1. Acesse **Genie** no menu lateral
2. Abra o Space: **"Yellow Taxi - Análise de Corridas"**
3. Faça perguntas em linguagem natural:
   - "Qual mês teve maior receita média?"
   - "Quantos passageiros em média às 18h em maio?"

---

## 📊 Respostas às Questões de Negócio

### Pergunta 1: Média de `total_amount` por mês

**Query**: `analysis/01_avg_total_amount_by_month.sql`

```sql
SELECT 
    DATE_TRUNC('month', tpep_pickup_datetime) AS trip_month,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    COUNT(*) AS num_trips
FROM workspace.silver.yellow_taxi_trips
WHERE total_amount > 0
GROUP BY trip_month
ORDER BY trip_month;
```

**Resultado esperado**: Tabela com média mensal de receita por corrida (Jan-Mai 2023)

### Pergunta 2: Média de `passenger_count` por hora em maio

**Query**: `analysis/02_avg_passenger_count_by_hour_may.sql`

```sql
SELECT 
    HOUR(tpep_pickup_datetime) AS trip_hour,
    ROUND(AVG(passenger_count), 2) AS avg_passenger_count,
    COUNT(*) AS num_trips
FROM workspace.silver.yellow_taxi_trips
WHERE DATE_TRUNC('month', tpep_pickup_datetime) = '2023-05-01'
  AND passenger_count > 0
GROUP BY trip_hour
ORDER BY trip_hour;
```

**Resultado esperado**: Tabela com média de passageiros por hora do dia (0-23h)

---

## 🛠️ Tecnologias Utilizadas

| Tecnologia | Uso |
|------------|-----|
| **Databricks** | Plataforma de analytics unificada |
| **Apache Spark / PySpark** | Processamento distribuído (ETL) |
| **Delta Lake** | Storage layer ACID transacional |
| **Unity Catalog** | Governança e metadados |
| **Serverless Compute** | Execução sem gerenciamento de clusters |
| **Genie AI/BI** | Interface conversacional para dados |
| **Python** | Linguagem principal (notebooks) |
| **SQL** | Queries analíticas |

---

## 📦 Dependências

Listadas em `requirements.txt`:

```
pyspark>=3.4.0
delta-spark>=2.4.0
pandas>=1.5.0
```

> **Nota**: O Databricks Runtime já inclui todas as bibliotecas principais. Dependências adicionais podem ser instaladas via `%pip install` nos notebooks.

---

## 🔍 Fonte dos Dados

**NYC Taxi and Limousine Commission (TLC)**
- URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Dataset: Yellow Taxi Trip Records
- Período: Janeiro a Maio de 2023
- Formato original: Parquet (~50-150 MB por mês)
- Total: ~15 milhões de corridas, ~3 GB descomprimido

---

## 📈 Diferenciais da Solução

✅ **Arquitetura moderna**: Medalhão (Bronze/Silver/Gold) com Delta Lake  
✅ **Governança**: Unity Catalog com lineage automático  
✅ **Escalabilidade**: PySpark para processamento distribuído  
✅ **Democratização**: Genie AI/BI para acesso sem código  
✅ **Reprodutibilidade**: Pipeline completo versionado no Git  
✅ **Boas práticas**: Código estruturado, testes, documentação  

---

## 🐛 Troubleshooting

| Problema | Solução |
|----------|---------|
| **Tabela não encontrada** | Execute os notebooks de ingestão em ordem (01 → 02 → 03) |
| **Timeout em download** | Reduza o número de meses ou use cluster tradicional |
| **Erro de permissão UC** | Verifique permissões no Catalog Explorer (GRANT SELECT) |
| **Volume não existe** | Crie via SQL: `CREATE VOLUME workspace.default.ifood_case` |

---

## 📧 Contato

Para dúvidas sobre este case técnico, abra uma issue no repositório ou entre em contato via workspace Databricks.

---

**Case Técnico Data Architect - iFood**  
**Última atualização**: 2024  
**Status**: ✅ Completo