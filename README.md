# Case Técnico Data Architect - iFood

## 📋 Sobre o Projeto

Este projeto realiza uma análise abrangente dos dados de corridas de táxi amarelo (Yellow Taxi) da cidade de Nova York, focando no período de janeiro a maio de 2023. O objetivo é extrair insights de negócio relevantes através da análise exploratória de dados e visualizações interativas.

---

## 📊 Visão Geral do Case

Este case técnico demonstra competências em:
- **Engenharia de Dados**: Pipeline completo de ingeção (Landing → Bronze → Silver → Gold)
- **PySpark**: Processamento distribuído de dados em grande escala
- **Arquitetura Medalhão**: Organização em camadas (Bronze/Silver/Gold)
- **Unity Catalog**: Governança e gestão de metadados
- **Databricks Serverless**: Computação elástica sem gerenciamento de clusters
- **Genie AI/BI**: Democratização de dados com IA generativa
- **Análise de Dados**: Resposta a questões de negócio com SQL e visualizações

---

## 🎯 Objetivos

* Analisar padrões de uso de táxis amarelos em NYC
* Identificar tendências temporais (por mês, dia e hora)
* Entender o comportamento dos passageiros
* Calcular métricas financeiras e operacionais
* Fornecer insights acionáveis para tomada de decisão

---

## 🛠️ Infraestrutura Databricks

### 🏗️ Databricks Workspace

O **Databricks Workspace** é o ambiente colaborativo onde todos os assets do projeto estão organizados:

```
/Workspace/Users/seu-usuario@email.com/
├── ifood-case/                    # Repositório Git (Git Folder)
│   ├── notebooks/                 # Notebooks de ingeção
│   ├── analysis/                  # Análises e queries
│   └── src/                       # Código fonte Python
├── Dashboards/                    # Lakeview Dashboards
└── Genie Spaces/                  # Espaços Genie AI/BI
```

**Características:**
- **Git Integration**: O repositório `ifood-case` é um **Git Folder** sincronizado com GitHub
- **Colaboração**: Múltiplos usuários podem editar notebooks simultaneamente
- **Versionamento**: Histórico completo de mudanças via Git
- **Organização**: Estrutura hierárquica de pastas

### 🗄️ Unity Catalog

O **Unity Catalog** é a camada de governança de dados do Databricks. No projeto, a hierarquia é:

```
Catalog: workspace
│
├── Schema: default
│   ├── Volume: ifood_case           # Landing Zone (arquivos parquet)
│   └── (tabelas temporárias)
│
├── Schema: bronze
│   └── Table: yellow_taxi_trips     # Dados brutos + metadados
│
├── Schema: silver
│   └── Table: yellow_taxi_trips     # Dados limpos e validados
│
└── Schema: gold
    ├── Table: avg_total_amount_by_month
    └── Table: avg_passenger_count_by_hour_may
```

#### 📌 Componentes do Unity Catalog

**1. Catalog (`workspace`)**
- **O que é**: Container de alto nível que agrupa schemas
- **Por que usamos**: Isola dados do projeto de outros workspaces
- **Permissões**: Controladas por grupos/usuários
- **Criação**: `CREATE CATALOG IF NOT EXISTS workspace`

**2. Schema (ou Database)**
- **O que é**: Namespace lógico para tabelas e volumes
- **Schemas usados neste projeto**:
  - `default`: Volumes e dados temporários
  - `bronze`: Camada de dados brutos
  - `silver`: Camada de dados limpos
  - `gold`: Camada de dados agregados (business layer)
- **Criação**: `CREATE SCHEMA IF NOT EXISTS workspace.bronze`

**3. Volume (`ifood_case`)**
- **O que é**: Armazenamento de arquivos não-estruturados (como um "bucket S3 gerenciado")
- **Tipo**: **Managed Volume** (Databricks gerencia o ciclo de vida)
- **Localização**: `/Volumes/workspace/default/ifood_case/`
- **Uso neste projeto**: 
  - **Landing Zone** para armazenar arquivos `.parquet` originais da NYC TLC
  - Estrutura:
    ```
    /Volumes/workspace/default/ifood_case/
    ├── landing/
    │   ├── yellow_tripdata_2023-01.parquet
    │   ├── yellow_tripdata_2023-02.parquet
    │   ├── yellow_tripdata_2023-03.parquet
    │   ├── yellow_tripdata_2023-04.parquet
    │   └── yellow_tripdata_2023-05.parquet
    ```
- **Vantagens**:
  - Governança via Unity Catalog (ACLs, audit logs)
  - Sem necessidade de gerenciar buckets S3 externos
  - Integração nativa com PySpark (`dbutils.fs.ls`, `spark.read.parquet`)

**4. Tables (Delta Lake)**
- **O que é**: Tabelas transacionais ACID armazenadas em Delta Lake
- **Formato**: Parquet + transaction log
- **Características**:
  - **ACID**: Transações atômicas, consistentes, isoladas e duráveis
  - **Time Travel**: Consultar versões históricas (`VERSION AS OF`, `TIMESTAMP AS OF`)
  - **Schema Evolution**: Adicionar/remover colunas sem reescrever dados
  - **Z-Ordering**: Otimização de leitura para colunas específicas
- **Localização gerenciada**: Databricks gerencia automaticamente

#### ⚙️ Por que Unity Catalog?

✅ **Governança Centralizada**
- Permissões unificadas (GRANT/REVOKE SQL)
- Audit logs de todos os acessos
- Lineage automático (rastreamento de dependências)

✅ **Portabilidade Multi-Cloud**
- Metadados independentes de cloud provider (AWS, Azure, GCP)
- Fácil migração entre ambientes

✅ **Simplicidade**
- Elimina necessidade de Hive Metastore externo
- Interface gráfica para navegação (Catalog Explorer)
- Integração nativa com Genie AI/BI

---

### ⚡ Databricks Serverless Compute

Este projeto utiliza **Databricks Serverless** como ambiente de execução. Entenda o que é, por que usamos, e suas limitações.

#### 🔍 O que é Serverless?

**Databricks Serverless** é um ambiente de computação totalmente gerenciado:
- **Zero configuração**: Não precisa criar ou gerenciar clusters
- **Instant start**: Inicia em segundos (vs minutos em clusters tradicionais)
- **Escala automática**: Ajusta recursos conforme a carga de trabalho
- **Pagamento por uso**: Cobra apenas pelo tempo de execução (por segundo)
- **Isolamento**: Cada notebook/query roda em ambiente isolado

**Arquitetura:**
```
Notebook/Query
    ↓
Serverless Runtime (Python 3.10, Spark 3.5)
    ↓
Pool elástico de recursos (CPU/Memória gerenciados pela Databricks)
    ↓
Armazenamento (Unity Catalog + Delta Lake)
```

#### ✅ Vantagens do Serverless

**1. Simplicidade Operacional**
- **Sem configuração**: Não precisa escolher tipo de instância, número de workers, autoscaling
- **Sem manutenção**: Databricks gerencia patches, updates, otimizações
- **Foco no código**: Desenvolvedores podem focar na lógica, não na infraestrutura

**2. Performance**
- **Start rápido**: ~5-10 segundos (vs 3-5 minutos em clusters tradicionais)
- **Otimizações automáticas**: Databricks aplica otimizações de query sob o capô
- **Cache inteligente**: Reutiliza recursos entre execuções quando possível

**3. Custo**
- **Pay-per-second**: Cobra apenas pelo tempo de execução real
- **Sem idle time**: Não paga por clusters parados
- **Escala para zero**: Recursos liberados automaticamente após uso
- **Ideal para dev/test**: Ambientes intermitentes (ex: Community Edition)

**4. Experiência de Desenvolvimento**
- **Execução instantânea**: Feedback rápido em loops de desenvolvimento
- **Sem espera**: Não precisa aguardar cluster iniciar/terminar
- **Ambiente consistente**: Todos os usuários usam mesma versão de runtime

#### ❌ Desvantagens e Limitações do Serverless

**1. Limitações de Recursos**
- **Timeout padrão**: 10 minutos por comando (configurável até 60 minutos)
- **Memória limitada**: Não configurável (gerenciado pela Databricks)
- **Paralelismo**: Menos controle sobre particionamento/paralelismo do Spark

**2. Workloads Não Recomendados**
- **Processamento de dados muito grandes**: Se o dataset for > 100 GB por partição
- **Streaming contínuo**: Streams 24/7 (melhor usar clusters dedicados)
- **Machine Learning pesado**: Treinamento distribuído de modelos grandes (ex: deep learning)
- **Jobs de produção críticos**: Preferência por clusters com SLA garantido

**3. Download de Arquivos Grandes**
- **Problema**: Downloads HTTP grandes (> 1 GB) podem falhar por:
  - Timeout de rede
  - Ambiente efêmero (nós podem ser reciclados)
- **Solução neste projeto**:
  - Arquivos parquet da NYC TLC são relativamente pequenos (~50-150 MB cada)
  - Se falhar, usar cluster tradicional apenas para a etapa de download

**4. Bibliotecas Personalizadas**
- **Limitação**: Não é possível instalar bibliotecas no nível do cluster (como em clusters tradicionais)
- **Solução**: Usar `%pip install` no início do notebook (instalação por sessão)
- **Impacto neste projeto**: Mínimo (usamos apenas bibliotecas já disponíveis no runtime)

**5. Acesso a Arquivos Locais**
- **Limitação**: Ambiente efêmero → arquivos escritos localmente (`/tmp`, `/dbfs/tmp`) não persistem
- **Solução**: Sempre salvar em Unity Catalog Volumes ou Delta Tables

#### 🆓 Serverless vs Cluster Tradicional

| Aspecto | Serverless | Cluster Tradicional |
|---------|------------|---------------------|
| **Setup** | Zero configuração | Requer configuração (tipo instância, workers) |
| **Start Time** | 5-10 segundos | 3-5 minutos |
| **Escala** | Automática | Manual ou autoscaling (mais lento) |
| **Custo (idle)** | $0 (escala para zero) | Cobra mesmo parado (se não configurar terminação) |
| **Controle** | Baixo (gerenciado) | Alto (configura tipo instância, workers, libs) |
| **Workloads grandes** | Não recomendado (> 100 GB) | Recomendado |
| **Streaming** | Limitado | Ideal |
| **Dev/Test** | **Ideal** | Overkill |
| **Produção** | Depende do workload | Preferível para crítico |

#### 🆓 Limitações da Community Edition

**Databricks Community Edition** é a versão gratuita do Databricks. Principais limitações:

❌ **Não permite criar clusters customizados**
- **Impacto**: Você **NÃO PODE** criar/configurar clusters tradicionais (All-Purpose ou Job Clusters)
- **Única opção de compute**: **Serverless** (fornecido automaticamente)
- **Por quê usamos Serverless**: Não é uma escolha, é a única opção disponível na Community Edition

❌ **Recursos limitados**
- **Memória/CPU**: Limitados (não divulgados publicamente)
- **Armazenamento**: Limitado (suficiente para projetos pequenos/médios)

❌ **Timeout de inatividade**
- **Sessão**: Desconecta após 2 horas de inatividade
- **Assets**: Notebooks/tabelas persistem, mas ambiente de execução é reciclado

✅ **O que FUNCIONA na Community Edition**
- Unity Catalog (Catalogs, Schemas, Tables, Volumes)
- Delta Lake
- PySpark (Python, SQL)
- Notebooks colaborativos
- Dashboards (Lakeview)
- **Genie AI/BI** (acesso limitado, mas funcional)
- SQL Warehouses (Serverless)

#### 💡 Por que este projeto usa Serverless

**Justificativa técnica:**

1. **Compatibilidade com Community Edition**
   - É a única opção disponível (não há acesso a clusters tradicionais)
   - Permite demonstrar competências sem necessidade de conta paga

2. **Adequado ao tamanho do dataset**
   - **Dados**: ~15 milhões de corridas (Jan-Mai 2023)
   - **Tamanho**: ~700 MB compressed parquet (≈ 2-3 GB descomprimido)
   - **Processamento**: Facilmente executável em Serverless

3. **Ciclo de desenvolvimento**
   - **Uso**: Desenvolvimento iterativo, testes, EDA
   - **Pattern**: Execuções intermitentes (não contínuas)
   - **Serverless é ideal para**: Prototipação e análise exploratória

4. **Demonstração de boas práticas**
   - Arquitetura modern-a (Unity Catalog + Delta Lake + Serverless)
   - Sem necessidade de gerenciar infraestrutura
   - Reprodutível por qualquer pessoa com Community Edition

**Quando migrar para Cluster Tradicional:**
- Dataset crescer para > 100 GB por partição
- Necessidade de streaming contínuo (24/7)
- Jobs de produção com SLA
- Treinamento de modelos de ML pesados

---

### 🤖 Genie AI/BI: Democratização de Dados

**Genie** é a interface de IA generativa do Databricks que permite fazer perguntas em **linguagem natural** sobre seus dados.

#### 🔍 O que é Genie?

**Genie AI/BI** é um agente conversacional que:
- **Entende perguntas em português/inglês**: "Qual foi a receita em março?"
- **Gera SQL automaticamente**: Traduz sua pergunta em queries SQL
- **Executa e visualiza**: Retorna resultados em tabelas e gráficos
- **Aprende com uso**: Melhora sugestões baseado em perguntas anteriores

**Arquitetura:**
```
Usuário: "Qual mês teve maior receita?"
    ↓
Genie (LLM)
    ↓
SQL gerado: SELECT trip_month, avg_total_amount 
            FROM workspace.gold.avg_total_amount_by_month 
            ORDER BY avg_total_amount DESC LIMIT 1
    ↓
Serverless SQL Warehouse (execução)
    ↓
Resultado: Abril - $29.50
    ↓
Visualização automática (gráfico/tabela)
```

#### 🎯 Genie Space: "Yellow Taxi - Análise de Corridas"

Neste projeto, foi criado um **Genie Space** conectado às tabelas Gold:

**Tabelas disponíveis no Space:**
- `workspace.gold.avg_total_amount_by_month`
- `workspace.gold.avg_passenger_count_by_hour_may`
- `workspace.silver.yellow_taxi_trips` (tabela completa para perguntas exploratórias)

**Perguntas prontas (Starter Questions):**
1. "Qual foi o valor médio de corrida em março de 2023?"
2. "Em que horário do dia há mais passageiros em maio?"
3. "Mostre a evolução do valor médio das corridas ao longo dos meses"
4. "Compare janeiro e maio em termos de receita"
5. "Qual horário tem menos passageiros em maio?"
6. "Mostre um gráfico da receita por mês"

**Exemplos de perguntas que você pode fazer:**
```
- "Qual mês teve maior receita média?"
- "Mostre um gráfico de barras dos passageiros por hora em maio"
- "Quantos passageiros em média às 8h da manhã em maio?"
- "Compare a receita de janeiro e abril"
- "Qual a tendência de receita ao longo dos meses?"
- "Em que horário há menos demanda em maio?"
```

#### ✅ Vantagens do Genie

**1. Democratização de Dados**
- **Usuários não-técnicos** podem explorar dados sem saber SQL
- **Analistas de negócio** ganham autonomia
- **Redução de backlog** da equipe de dados

**2. Produtividade**
- **Respostas instantâneas**: Não precisa escrever SQL manualmente
- **Visualizações automáticas**: Gráficos criados automaticamente
- **Iteração rápida**: Refinamento de perguntas em tempo real

**3. Aprendizado**
- **Transparência**: Mostra a query SQL gerada (usuários aprendem SQL!)
- **Sugestões**: Oferece perguntas relacionadas
- **Histórico**: Salva perguntas anteriores para referência

**4. Governança**
- **Conectado ao Unity Catalog**: Respeita permissões (users só veem dados que têm acesso)
- **Audit logs**: Todas as queries são registradas
- **Lineage**: Rastreamento de dependências mantido

#### 💡 Como Usar o Genie Space

**Acessar:**
1. Menu lateral esquerdo → **"Genie"** 🤖
2. Procurar: **"Yellow Taxi - Análise de Corridas"**
3. Clicar no nome do Space

**Fazer perguntas:**
1. **Clicar em uma pergunta sugerida** (mais rápido)
2. **Ou digitar sua própria pergunta** na caixa de texto
3. Aguardar 5-10 segundos
4. Ver resultados em 3 abas:
   - **Table**: dados tabulares
   - **Visualization**: gráficos automáticos
   - **SQL**: query gerada (para aprender/auditar)

**Dicas para boas perguntas:**
- ✅ Seja específico: "mostre a receita média por mês em 2023"
- ✅ Use palavras-chave dos dados: "receita", "passageiros", "hora", "maio"
- ✅ Peça visualizações: "faça um gráfico de linha da receita"
- ✅ Compare períodos: "compare janeiro e maio"
- ❌ Evite perguntas vagas: "mostre os dados"

#### 🎬 Demonstração de Valor para o Case

**Por que Genie é relevante para o case:**

1. **Responde questões de negócio sem escrever SQL**
   - Pergunta 1: "Qual a média de total_amount por mês?" → Genie responde
   - Pergunta 2: "Quantos passageiros em média por hora em maio?" → Genie responde

2. **Demonstra arquitetura moderna**
   - Unity Catalog + Genie = democratização de dados
   - Self-service analytics
   - Redução de dependência de analistas

3. **Facilita validação**
   - Avaliadores podem explorar dados livremente
   - Não precisam executar notebooks
   - Interface intuitiva

**Limitações do Genie (Community Edition):**
- Acesso pode ser limitado (depende da versão do workspace)
- Algumas features avançadas (custom instructions) podem não estar disponíveis

---

## 🗂️ Estrutura do Repositório

```
ifood-case/
├── README.md                          # Este arquivo
├── requirements.txt                   # Dependências do projeto
├── .gitignore                         # Arquivos ignorados pelo Git
├── src/                               # Código fonte da solução
│   ├── config.py                      # Configurações gerais
│   ├── bronze/                        # Transformações Bronze
│   ├── silver/                        # Transformações Silver
│   ├── gold/                          # Transformações Gold
│   └── common/                        # Utilidades comuns
├── notebooks/                         # Notebooks de ingestão
│   ├── 00_download_landing.ipynb      # Download dos dados
│   ├── 00_setup_and_config.ipynb      # Setup e configuração
│   ├── 01_landing_to_bronze.ipynb     # Ingestão Bronze
│   ├── 02_bronze_to_silver.ipynb      # Transformação Silver
│   └── 03_silver_to_gold.ipynb        # Agregações Gold
└── analysis/                          # Análises e respostas
    ├── 00_business_questions.ipynb    # Questões de negócio
    ├── 01_avg_total_amount_by_month.ipynb      # Análise 1: Média mensal
    ├── 02_avg_passenger_count_by_hour_may.ipynb # Análise 2: Média por hora
    └── 03_exploratory_analysis.ipynb  # Análise exploratória
```

## 🚀 Como Utilizar

### Pré-requisitos

* Databricks Workspace (Community Edition ou superior)
* Dados do NYC Yellow Taxi Trip Records (Jan-Mai 2023)
* Compute serverless ou cluster Databricks configurado

### Passo 1: Configuração Inicial

1. **Clone ou importe o repositório** no seu Databricks Workspace
2. **Execute o notebook de setup**:
   ```
   notebooks/00_setup_and_config.ipynb
   ```
   Este notebook:
   * Valida o ambiente Databricks
   * Cria estruturas Unity Catalog (catalog, schema, volume)
   * Configura paths e variáveis

### Passo 2: Ingestão de Dados

Execute os notebooks de ingestão em ordem:

#### 1. Download (00_download_landing.ipynb)
* Faz download dos dados do NYC TLC
* Armazena na Landing Zone (UC Volume)
* Período: Janeiro a Maio de 2023

#### 2. Bronze (01_landing_to_bronze.ipynb)
* Lê arquivos parquet da Landing Zone
* Adiciona metadados de controle
* Salva em Delta Lake (camada Bronze)

#### 3. Silver (02_bronze_to_silver.ipynb)
* Limpa e valida dados
* Padroniza tipos e formatos
* Remove duplicatas
* Garante colunas obrigatórias: VendorID, passenger_count, total_amount, tpep_pickup_datetime, tpep_dropoff_datetime

#### 4. Gold (03_silver_to_gold.ipynb)
* Cria agregações de negócio
* Tabelas otimizadas para análise
* Métricas por mês, hora, zona

### Passo 3: Análises

Execute os notebooks em `analysis/` para responder às questões de negócio:

#### 1. Business Questions (00_business_questions.ipynb)
* Define as principais questões de negócio
* Estabelece métricas e KPIs

#### 2. Média de Valor por Mês (01_avg_total_amount_by_month.ipynb)
**Pergunta:** Qual a média de valor total (total_amount) recebido em um mês considerando todos os yellow táxis da frota?

#### 3. Média de Passageiros por Hora em Maio (02_avg_passenger_count_by_hour_may.ipynb)
**Pergunta:** Qual a média de passageiros (passenger_count) por cada hora do dia que pegaram táxi no mês de maio?

#### 4. Análise Exploratória Completa (03_exploratory_analysis.ipynb)
* Análise detalhada de todas as variáveis
* Visualizações e gráficos interativos
* Correlações e insights estatísticos

## 📊 Principais Análises

### Métricas Calculadas

* **Valor médio por corrida** (total_amount)
* **Número médio de passageiros** por corrida e por período
* **Distância média** das viagens
* **Gorjetas médias** e taxa de gorjeta
* **Distribuição temporal** das corridas

### Dimensões de Análise

* **Temporal**: Mês, dia da semana, hora do dia
* **Geográfica**: Zonas de pickup e dropoff
* **Financeira**: Valores, gorjetas, taxas
* **Operacional**: Número de passageiros, distância, duração

## 🛠️ Tecnologias Utilizadas

* **Databricks**: Plataforma de análise de dados
* **Apache Spark / PySpark**: Processamento distribuído
* **Delta Lake**: Armazenamento ACID transacional
* **Unity Catalog**: Governança de dados
* **Python**: Análise e transformações
* **SQL**: Queries analíticas

## 📦 Dependências

As dependências estão listadas em `requirements.txt`. O Databricks Runtime já inclui a maioria das bibliotecas necessárias:

* PySpark >= 3.4.0
* Delta Spark >= 2.4.0
* Pandas >= 1.5.0

## 🔍 Fonte dos Dados

Os dados são do **NYC Taxi and Limousine Commission (TLC)**, disponíveis publicamente em:
* Dataset: Yellow Taxi Trip Records
* URL: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
* Período: Janeiro a Maio de 2023
* Formato: Parquet

## 🏗️ Arquitetura

O projeto segue a **arquitetura medalhão** (Bronze/Silver/Gold):

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
Análises e Dashboards
```

## 📈 Resultados Esperados

Ao final das análises, você terá:

✅ Pipeline de ingestão completo e automatizado  
✅ Dados organizados em camadas (Bronze/Silver/Gold)  
✅ Respostas para as questões de negócio  
✅ Compreensão profunda dos padrões de uso de táxis em NYC  
✅ Identificação de horários e períodos de maior demanda  
✅ Insights sobre comportamento de passageiros e pagamentos  

## 🤝 Como Contribuir

1. Crie uma branch para sua feature: `git checkout -b feature/nova-analise`
2. Commit suas mudanças: `git commit -m 'Adiciona nova análise X'`
3. Push para a branch: `git push origin feature/nova-analise`
4. Abra um Pull Request

## 📝 Boas Práticas

* Execute os notebooks em ordem sequencial
* Valide os dados antes de cada transformação
* Documente novos insights nos notebooks
* Mantenha o código limpo e comentado
* Atualize este README ao adicionar novas análises

## 🐛 Troubleshooting

### Erro: Tabela não encontrada
* Verifique se os notebooks de ingestão foram executados
* Confirme o caminho correto dos dados no Unity Catalog

### Erro: Compute não disponível
* Certifique-se que o serverless compute está habilitado
* Ou configure um cluster Databricks adequado

### Dados incompletos ou vazios
* Verifique se o download foi concluído com sucesso
* Confirme as permissões de acesso aos dados
* Valide o período de análise configurado

## 📧 Contato

Para dúvidas ou sugestões sobre este projeto, entre em contato através do workspace Databricks.

## 📄 Licença

Este projeto é de uso educacional e analítico.

---

**Case Técnico:** Data Architect - iFood  
**Última atualização:** 2024  
**Versão:** 1.0.0  
**Status:** ✅ Ativo