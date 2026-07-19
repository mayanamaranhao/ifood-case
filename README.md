# 🚕 Yellow Taxi NYC - Análise de Dados

## 📋 Sobre o Projeto

Este projeto realiza uma análise abrangente dos dados de corridas de táxi amarelo (Yellow Taxi) da cidade de Nova York, focando no período de janeiro a maio de 2023. O objetivo é extrair insights de negócio relevantes através da análise exploratória de dados e visualizações interativas.

## 🎯 Objetivos

* Analisar padrões de uso de táxis amarelos em NYC
* Identificar tendências temporais (por mês, dia e hora)
* Entender o comportamento dos passageiros
* Calcular métricas financeiras e operacionais
* Fornecer insights acionáveis para tomada de decisão

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