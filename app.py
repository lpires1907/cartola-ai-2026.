import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import json
from google.oauth2 import service_account

# --- CONFIGURAÇÕES DE PÁGINA ---
st.set_page_config(page_title="Cartola SAS Analytics 2026", page_icon="⚽", layout="wide")

# --- CONEXÃO BIGQUERY ---
def get_bq_client():
    if os.getenv('GCP_SERVICE_ACCOUNT'):
        info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info['project_id'])
    return bigquery.Client()

client = get_bq_client()
DATASET_ID = "cartola_analytics"

# --- CARREGAMENTO DE DADOS ---
@st.cache_data(ttl=600)
def load_data(query):
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        return pd.DataFrame()

# --- INTERFACE ---
st.title("🏆 Cartola SAS Brasil - Analytics")

# Abas atualizadas
tab1, tab2, tab3 = st.tabs(["⚽ Liga SAS Brasil 2026", "🏆 Mata-Mata", "🎤 Narrador IA"])

# --- ABA 1: LIGA SAS BRASIL 2026 ---
with tab1:
    st.header("Classificação Geral - Pontos Corridos")
    df_view = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.view_consolidada_times`")
    
    if not df_view.empty:
        col1, col2, col3 = st.columns(3)
        lider = df_view.iloc[0]
        col1.metric("🥇 Líder Geral", lider['nome'], f"{lider['total_geral']:.2f} pts")
        
        top_mitada = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
        col2.metric("🚀 Maior Mitada", top_mitada['nome'], f"{top_mitada['maior_pontuacao']:.2f} pts")
        
        rico = df_view.sort_values('patrimonio_atual', ascending=False).iloc[0]
        col3.metric("💰 Mais Rico", rico['nome'], f"C$ {rico['patrimonio_atual']:.2f}")

        st.divider()
        st.dataframe(
            df_view[['nome', 'nome_cartola', 'total_geral', 'media', 'maior_pontuacao', 'rodadas_jogadas']],
            use_container_width=True, hide_index=True
        )
    else:
        st.warning("Dados da Liga SAS Brasil ainda não processados.")

# --- ABA 2: MATA-MATA ---
with tab2:
    st.header("Copas e Eliminatórias")
    df_copa = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.copa_mata_mata` ORDER BY data_coleta DESC")
    
    if not df_copa.empty:
        copas_disponiveis = df_copa['nome_copa'].unique()
        copa_sel = st.selectbox("Selecione a Copa:", copas_disponiveis)
        df_filtro = df_copa[df_copa['nome_copa'] == copa_sel]
        
        for fase in df_filtro['fase_copa'].unique():
            with st.expander(f"📍 {fase}", expanded=True):
                jogos = df_filtro[df_filtro['fase_copa'] == fase]
                for _, jogo in jogos.iterrows():
                    c1, c2, c3 = st.columns([2, 1, 2])
                    c1.write(f"**{jogo['time_a_nome']}**")
                    c2.write(f"{jogo['time_a_pontos']:.2f} x {jogo['time_b_pontos']:.2f}")
                    c3.write(f"**{jogo['time_b_nome']}**")
    else:
        st.info("Nenhuma copa ativa ou dados não encontrados na tabela `copa_mata_mata`.")

# --- ABA 3: NARRADOR IA ---
with tab3:
    st.header("🎤 Resenha do Narrador")
    df_ia = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.comentarios_ia` ORDER BY data DESC LIMIT 10")
    if not df_ia.empty:
        for _, row in df_ia.iterrows():
            with st.chat_message("assistant", avatar="🎤"):
                st.write(f"**Rodada {row['rodada']} ({row['tipo']})**")
                st.write(row['texto'])
                st.caption(f"🕒 {row['data']}")
    else:
        st.write("O narrador está preparando a garganta...")
