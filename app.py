import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import plotly.express as px

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Cartola Analytics 2026", page_icon="⚽", layout="wide")

# CSS para esconder índices e melhorar visual
st.markdown("""
<style>
    .metric-card {background-color: #f0f2f6; padding: 15px; border-radius: 10px; text-align: center;}
    [data-testid="stMetricValue"] {font-size: 1.8rem !important;}
</style>
""", unsafe_allow_html=True)

# --- CONEXÃO BQ (BLINDADA) ---
@st.cache_resource
def get_bq_client():
    # 1. Tenta credenciais locais (desenvolvimento)
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    
    # 2. Tenta Secrets do Streamlit (Produção)
    try:
        if "GCP_SERVICE_ACCOUNT" not in st.secrets:
            st.error("❌ Secret 'GCP_SERVICE_ACCOUNT' não encontrado.")
            st.stop()

        service_account_info = st.secrets["GCP_SERVICE_ACCOUNT"]

        # Verifica se veio como string (JSON texto) ou dict (Objeto já convertido)
        if isinstance(service_account_info, str):
            info = json.loads(service_account_info)
        else:
            info = service_account_info

        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info['project_id'])
        
    except Exception as e:
        st.error(f"Erro de Autenticação: {e}")
        st.stop()

client = get_bq_client()

# --- CARGA DE DADOS ---
@st.cache_data(ttl=600)
def load_data():
    try:
        # 1. View Consolidada
        query_view = "SELECT * FROM `cartola_analytics.view_consolidada_times`"
        df_view = client.query(query_view).to_dataframe()
        
        # --- LIMPEZA DE TIPOS (Evita TypeError) ---
        cols_numericas = ['total_geral', 'pontos_turno_1', 'pontos_turno_2', 'maior_pontuacao', 'menor_pontuacao']
        
        for col in cols_numericas:
            if col not in df_view.columns:
                df_view[col] = 0.0
            else:
                df_view[col] = pd.to_numeric(df_view[col], errors='coerce').fillna(0.0)

        # Limpeza dinâmica para meses
        cols_meses = [c for c in df_view.columns if 'pontos_' in c]
        for col in cols_meses:
             df_view[col] = pd.to_numeric(df_view[col], errors='coerce').fillna(0.0)
        # ------------------------------------------

        # 2. Metadados da Rodada Atual
        query_meta = """
            SELECT MAX(rodada) as rodada_atual, 
                (SELECT Mensal FROM `cartola_analytics.Rodada_Mensal` 
                    WHERE Rodada = (SELECT MAX(rodada) FROM `cartola_analytics.historico`)) as mes_atual
            FROM `cartola_analytics.historico`
        """
        df_meta = client.query(query_meta).to_dataframe()
        rodada = int(df_meta['rodada_atual'].iloc[0]) if not df_meta.empty and pd.notnull(df_meta['rodada_atual'].iloc[0]) else 1
        mes_raw = df_meta['mes_atual'].iloc[0] if not df_meta.empty and pd.notnull(df_meta['mes_atual'].iloc[0]) else "Jan Fev"
        
        map_mes = {
            'Jan Fev': 'pontos_jan_fev', 'Março': 'pontos_marco', 'Abril': 'pontos_abril',
            'Maio': 'pontos_maio', 'Jun Jul': 'pontos_jun_jul', 'Agosto': 'pontos_agosto',
            'Setembro': 'pontos_setembro', 'Outubro': 'pontos_outubro', 'Nov Dez': 'pontos_nov_dez'
        }
        col_mes = map_mes.get(mes_raw, 'total_geral')

        return df_view, rodada, mes_raw, col_mes

    except Exception as e:
        st.error(f"Erro ao carregar dados do BigQuery: {e}")
        return pd.DataFrame(), 0, "Erro", "total_geral"

# Carrega os dados
with st.spinner('Carregando dados do Cartola...'):
    df_view, rodada_atual, nome_mes_atual, col_mes_atual
