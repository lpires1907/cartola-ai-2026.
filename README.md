# 🏆 Cartola Analytics - Liga SAS Brasil 2026

![Python](https://img.shields.io/badge/Python-3.12-blue?style=for-the-badge&logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=Streamlit)
![BigQuery](https://img.shields.io/badge/Google_BigQuery-669DF6?style=for-the-badge&logo=googlebigquery)
![Gemini AI](https://img.shields.io/badge/Google_Gemini_AI-8E75B2?style=for-the-badge&logo=google)

Este projeto é uma **pipeline de engenharia de dados completa** e um **dashboard interativo** para acompanhar, analisar e narrar (via Inteligência Artificial) os resultados da **Liga SAS Brasil** no Cartola FC.

O sistema coleta dados automaticamente, armazena em um Data Warehouse (BigQuery), gera insights com IA Generativa e apresenta tudo em uma aplicação Web.

---

## 🚀 Funcionalidades

### 1. 📊 Dashboard Interativo (Streamlit)
- **Classificação Geral:** Tabelas e gráficos de liderança, turnos e mensal.
- **KPIs:** Destaques para Líder, Vice, Maior Pontuação (Mitada) e Menor Pontuação (Zicada).
- **Raio-X da Rodada:** Detalhes de escalação de cada time.
- **Visualização de Copas:** Acompanhamento gráfico de chaves de mata-mata.

### 2. 🤖 Narrador IA (Google Gemini)
- **Narração da Rodada:** Comentários sarcásticos e divertidos sobre o desempenho dos times na rodada.
- **Análise de Temporada:** Visão macro sobre quem domina o campeonato (Geral, Turno e Mês).
- **Integração:** Os textos são gerados via API do Gemini e salvos no banco para exibição no App.

### 3. ⚔️ Coleta de Copas (Mata-Mata)
- Sistema genérico para monitorar múltiplas copas simultaneamente.
- Configuração via arquivo JSON (`copas.json`).
- Histórico de confrontos, placares e vencedores.

### 4. ⚙️ Engenharia de Dados
- **Pipeline Automatizado:** Execução via GitHub Actions (Cron Jobs).
- **Data Warehouse:** Armazenamento robusto no Google BigQuery.
- **Processamento:** Limpeza de dados, remoção de duplicatas e criação de Views consolidadas.

---

## 🛠️ Arquitetura do Projeto

O fluxo de dados segue a seguinte ordem:

1.  **Coleta (`coletor.py` e `coletor_copa.py`):** Acessa a API do Cartola FC usando Token de Autenticação.
2.  **Armazenamento:** Salva os dados brutos e históricos no **Google BigQuery**.
3.  **Processamento (`processamento.py`):** Higieniza os dados, recria Views SQL e atualiza tabelas de metadados.
4.  **Enriquecimento (`narrador.py`):** Envia estatísticas para o **Google Gemini** e recebe textos narrativos.
5.  **Apresentação (`app.py`):** O Streamlit lê as Views do BigQuery e exibe o Dashboard.

---

## 📂 Estrutura de Arquivos

```text
├── .github/workflows/main.yml  # Automação do Pipeline (GitHub Actions)
├── app.py                      # Aplicação Dashboard (Streamlit)
├── main.py                     # Orquestrador principal do Pipeline
├── coletor.py                  # Coleta dados da Liga de Pontos Corridos
├── coletor_copa.py             # Coleta dados das Copas Mata-Mata
├── processamento.py            # Lógica de limpeza e Views SQL
├── narrador.py                 # Integração com IA (Gemini)
├── copas.json                  # Configuração das Copas ativas
├── requirements.txt            # Dependências do Python
└── README.md                   # Documentação
