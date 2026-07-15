# Case Técnico Data Architect — iFood

Solução de ingestão, modelagem e disponibilização dos dados de corridas de Yellow Taxi de NYC (Jan–Mai/2023), usando **arquitetura medalhão** (Bronze / Silver / Gold) em PySpark + Delta Lake + **Genie AI/BI** para democratização de dados.

---

## 🏗️ Arquitetura

```
NYC TLC (parquet) 
    ↓
Landing Zone (UC Volume) 
    ↓
Bronze (dados brutos + metadados)
    ↓
Silver (dados limpos, tipados, deduplicados)
    ↓
Gold (agregações de negócio)
    ↓
🤖 Genie Space (perguntas em linguagem natural)
```

### Camadas da Arquitetura Medalhão

| Camada | Conteúdo | Formato | Localização | Propósito |
|--------|----------|---------|-------------|-----------|
| **Landing** | Arquivos originais intocados | Parquet | UC Volume: \`/Volumes/workspace/default/ifood_case/landing/yellow_taxi\` | Reprocessamento sem depender da fonte |
| **Bronze** | Raw + metadados técnicos | Delta | \`workspace.bronze.yellow_taxi_trips\` | Histórico bruto, schema-on-read |
| **Silver** | Tipado, limpo, deduplicado | Delta | \`workspace.silver.yellow_taxi_trips\` | Fonte única de verdade |
| **Gold** | Agregados de negócio | Delta | \`workspace.gold.*\` | Respostas prontas para consumo |
| **🤖 Genie** | Interface de perguntas | AI/BI | Genie Space | Self-service analytics |

---

## 📁 Estrutura do Repositório

A estrutura de código **espelha a arquitetura medalhão** no Unity Catalog:

```
ifood-case/
├── src/
│   ├── bronze/              ↔️  workspace.bronze
│   │   ├── download_taxi_data.py
│   │   └── landing_to_bronze.py
│   │
│   ├── silver/              ↔️  workspace.silver
│   │   └── bronze_to_silver.py
│   │
│   ├── gold/                ↔️  workspace.gold
│   │   └── silver_to_gold.py
│   │
│   ├── common/
│   │   ├── spark_session.py
│   │   ├── schemas.py
│   │   └── utils.py
│   │
│   └── config.py
│
├── notebooks/
│   ├── 00_download_landing.ipynb
│   ├── 01_landing_to_bronze.ipynb
│   ├── 02_bronze_to_silver.ipynb
│   └── 03_silver_to_gold.ipynb
│
└── README.md
```

### Por que Espelhar a Estrutura?

✅ **Navegação intuitiva**: problema no bronze? → \`src/bronze/\`  
✅ **Escalabilidade**: cada camada cresce independentemente  
✅ **Clareza**: código reflete visualmente a arquitetura de dados  
✅ **Manutenibilidade**: alinhamento com melhores práticas de Data Engineering  

---

## 🚀 Como Executar

### 1. Setup Inicial

Clone o repositório via **Databricks Repos**:
- **Repos** → **Add Repo** → cole a URL do GitHub
- O repo fica em \`/Workspace/Repos/<seu-usuario>/ifood-case\`

### 2. Criar Volume (Landing Zone)

**Catalog** → catalog \`workspace\` → schema \`default\` → **Create Volume**
- Nome: \`ifood_case\`
- Confirme que o path bate com \`LAKE.landing_path\` em \`src/config.py\`

### 3. Executar Notebooks na Sequência

Use compute **serverless** (já vem selecionado por padrão):

```python
# Notebook 00: Download dos arquivos
from src.bronze import download_taxi_data
download_taxi_data.run()

# Notebook 01: Landing → Bronze
from src.bronze import landing_to_bronze
landing_to_bronze.run()

# Notebook 02: Bronze → Silver
from src.silver import bronze_to_silver
bronze_to_silver.run()

# Notebook 03: Silver → Gold
from src.gold import silver_to_gold
silver_to_gold.run()
```

**Nota**: Se \`download_taxi_data.run()\` falhar por restrição de rede, baixe os 5 arquivos manualmente do [NYC TLC](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) e suba via **Catalog → Volume → Upload**.

---

## 🤖 Genie Space — Self-Service Analytics

### O Que É?

Um **Genie Space** é uma interface de perguntas em linguagem natural alimentada por IA que permite que **qualquer pessoa** faça análises sem precisar saber SQL.

### Acesso ao Genie Space

**Nome**: Yellow Taxi - Análise de Corridas  
**Link**: Acesse via **AI/BI Dashboards** → **Genie Spaces** → **Yellow Taxi - Análise de Corridas**

Ou diretamente pela URL no seu workspace Databricks:
```
/genie/rooms/01f180845486195bbde9c492bc4d7531
```

### Tabelas Conectadas

O Genie Space consulta automaticamente as seguintes tabelas Gold:
- \`workspace.gold.avg_total_amount_by_month\` — Receita média mensal
- \`workspace.gold.avg_passenger_count_by_hour_may\` — Passageiros médios por hora em maio

### Como Usar

#### **Opção 1: Clicar nas Starter Questions**

Ao abrir o Genie Space, você verá perguntas sugeridas:
- "Qual foi o valor médio de corrida em março de 2023?"
- "Em que horário do dia há mais passageiros em maio?"
- "Mostre a evolução do valor médio das corridas ao longo dos meses"
- "Qual foi o mês com maior receita média?"
- "Compare a média de passageiros entre manhã e noite em maio"
- "Qual horário tem menos passageiros em maio de 2023?"

#### **Opção 2: Fazer Perguntas Livres**

Digite suas perguntas em linguagem natural:
```
"Qual mês teve maior receita?"
"Mostre um gráfico da receita por mês"
"Quantos passageiros em média às 8h da manhã em maio?"
"Compare janeiro e maio"
"Qual a tendência de receita ao longo dos meses?"
```

### Recursos do Genie

✅ **Gráficos automáticos** — visualizações inteligentes (linha, barras, área)  
✅ **Respostas instantâneas** — traduz linguagem natural → SQL automaticamente  
✅ **Tabelas + gráficos** — resultados em múltiplos formatos  
✅ **Download de dados** — exportar resultados  
✅ **Histórico de conversas** — todas as perguntas são salvas  

### Benefícios

🎯 **Democratização de dados**: qualquer pessoa analisa sem SQL  
🚀 **Self-service analytics**: reduz carga dos analistas de dados  
📊 **Insights rápidos**: respostas em segundos  
🔒 **Governança**: Unity Catalog controla acessos automaticamente  

---

## 📊 Perguntas de Negócio Respondidas

### 1. Qual a média de \`total_amount\` por mês?

**Tabela Gold**: \`workspace.gold.avg_total_amount_by_month\`

```sql
SELECT * 
FROM workspace.gold.avg_total_amount_by_month
ORDER BY trip_year, trip_month;
```

**Ou via Genie**: "Mostre a evolução do valor médio das corridas ao longo dos meses"

### 2. Qual a média de \`passenger_count\` por hora do dia em maio?

**Tabela Gold**: \`workspace.gold.avg_passenger_count_by_hour_may\`

```sql
SELECT * 
FROM workspace.gold.avg_passenger_count_by_hour_may
ORDER BY pickup_hour;
```

**Ou via Genie**: "Em que horário do dia há mais passageiros em maio?"

---

## 🎯 Decisões Técnicas

### Por que Arquitetura Medalhão?

- **Bronze imutável**: erros de limpeza na Silver são corrigíveis via reprocessamento
- **Silver como contrato único**: evita reimplementação de regras de qualidade
- **Gold pré-agregada**: respostas de negócio como tabelas simples de SELECT

### Por que Delta Lake?

- **ACID transactions**: garantia de consistência
- **Schema evolution**: \`overwriteSchema\` para evoluções controladas
- **Time travel**: auditoria e rollback
- **MERGE/upsert**: suporte nativo para ingestão incremental futura

### Por que Genie Space?

- **Democratização**: usuários de negócio acessam dados sem SQL
- **Redução de backlog**: analistas focam em problemas complexos
- **Time-to-insight**: respostas instantâneas vs. dias de fila
- **Governança**: Unity Catalog gerencia permissões automaticamente

### Qualidade de Dados

Regras aplicadas na camada Silver (\`src/silver/bronze_to_silver.py\`):
- \`passenger_count > 0\`
- \`total_amount >= 0\`
- \`trip_distance >= 0\`
- \`tpep_dropoff_datetime >= tpep_pickup_datetime\`
- Deduplicação por (\`VendorID\`, \`tpep_pickup_datetime\`, \`tpep_dropoff_datetime\`)

---

## 🔄 Possíveis Evoluções

### Curto Prazo
- [ ] Adicionar mais agregações Gold (receita por vendor, distância média, etc.)
- [ ] Criar dashboards Lakeview conectados às tabelas Gold
- [ ] Implementar testes unitários (pytest) para transformações

### Médio Prazo
- [ ] Ingestão incremental via Auto Loader (substitui \`overwrite\` por \`append\`)
- [ ] Orquestração via Databricks Workflows com retries e alertas
- [ ] Testes de qualidade automatizados (Great Expectations)

### Longo Prazo
- [ ] Data Quality checks como Delta constraints
- [ ] Linhagem completa via Unity Catalog
- [ ] Extender para outros datasets (green taxi, for-hire vehicles)

---

## 📚 Referências

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Databricks Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Unity Catalog](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Lake](https://delta.io/)
- [Databricks Genie Spaces](https://docs.databricks.com/genie/index.html)

---

## 📝 Licença

MIT License — veja arquivo \`LICENSE\` para detalhes.

---

## 👤 Autor

Projeto desenvolvido como case técnico para vaga de Data Architect no iFood.

**Stack**: PySpark • Delta Lake • Unity Catalog • Databricks • Genie AI/BI
