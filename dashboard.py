import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# --- CONFIG ---
st.set_page_config(page_title="Cartola AI 2026", layout="wide", page_icon="⚽")

# --- CONEXÃO BIGQUERY ---
@st.cache_resource
def get_client():
    try:
        # Lê o JSON dos Secrets do Streamlit (formato TOML/String)
        service_account_info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        return bigquery.Client(credentials=credentials, project=service_account_info['project_id'])
    except Exception as e:
        st.error(f"Erro de Conexão GCP: {e}")
        return None

@st.cache_data(ttl=600) # Cache de 10 min
def carregar_dados():
    client = get_client()
    if not client: return pd.DataFrame(), pd.DataFrame()

    # Pega o Project ID automaticamente da credencial
    project_id = client.project
    dataset_id = "cartola_analytics" # Mesma ID usada no coletor

    try:
        # Busca histórico
        q_hist = f"SELECT * FROM `{project_id}.{dataset_id}.historico` ORDER BY timestamp ASC"
        df_hist = client.query(q_hist).to_dataframe()

        # Busca corneta
        q_corneta = f"SELECT * FROM `{project_id}.{dataset_id}.comentarios_ia` ORDER BY data DESC LIMIT 1"
        df_corneta = client.query(q_corneta).to_dataframe()
        
        return df_hist, df_corneta
    except Exception as e:
        # Se a tabela ainda não existir (primeira execução)
        return pd.DataFrame(), pd.DataFrame()

# --- INTERFACE ---
st.title("⚽ Dashboard Cartola 2026")
st.markdown("Monitoramento em tempo real da Liga via BigQuery & Gemini AI")

df_hist, df_corneta = carregar_dados()

if not df_hist.empty:
    # IA Section
    if not df_corneta.empty:
        st.success(f"🤖 **Comentário da Rodada:** {df_corneta['texto'].iloc[0]}")
    
    st.divider()

    # Métricas
    ultimo_ts = df_hist['timestamp'].max()
    ranking_atual = df_hist[df_hist['timestamp'] == ultimo_ts].sort_values(by='pontos', ascending=False)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    col1.metric("🥇 Líder", ranking_atual.iloc[0]['nome'], f"{ranking_atual.iloc[0]['pontos']:.2f}")
    col2.metric("🥈 Vice-Líder", ranking_atual.iloc[1]['nome'], f"{ranking_atual.iloc[1]['pontos']:.2f}")
    col3.metric("🕒 Atualizado", ultimo_ts.strftime('%d/%m às %H:%M'))
    col4.metric("🥉 Vice-Lanterna", ranking_atual.iloc[-2]['nome'], f"{ranking_atual.iloc[-2]['pontos']:.2f}")
    col5.metric("🐌 Lanterna", ranking_atual.iloc[-1]['nome'], f"{ranking_atual.iloc[-1]['pontos']:.2f}")

    # Gráficos e Tabelas
    tab1, tab2, tab3 = st.tabs(["📈 Evolução", "📋 Tabela Detalhada", "📊 Filtro por Rodada"])
    
    with tab1:
        # Pivot table para o gráfico de linhas
        chart_data = df_hist.pivot(index='timestamp', columns='nome', values='pontos')
        st.line_chart(chart_data)
        
    with tab2:
        st.dataframe(ranking_atual[['nome', 'nome_cartola', 'pontos', 'patrimonio']], use_container_width=True, hide_index=True)
    
    with tab3:
        st.subheader("Pontuações por Rodada")
        
        # Obter lista de timestamps (rodadas)
        rodadas = sorted(df_hist['timestamp'].unique(), reverse=True)
        
        # Selector de rodada
        rodada_selecionada = st.selectbox(
            "Selecione a rodada:",
            rodadas,
            format_func=lambda x: x.strftime('%d/%m/%Y às %H:%M')
        )
        
        # Filtrar dados da rodada selecionada
        df_rodada = df_hist[df_hist['timestamp'] == rodada_selecionada].sort_values(by='pontos', ascending=False)
        
        # Exibir tabela com coluna de posição
        df_rodada_exibir = df_rodada[['nome', 'nome_cartola', 'pontos', 'patrimonio']].reset_index(drop=True)
        df_rodada_exibir.index = df_rodada_exibir.index + 1
        df_rodada_exibir.index.name = 'Posição'
        
        st.dataframe(df_rodada_exibir, use_container_width=True)

else:
    st.info("🚧 Aguardando dados... O coletor do GitHub Actions ainda não rodou ou as tabelas estão vazias.")
    if st.button("Recarregar"):
        st.rerun()