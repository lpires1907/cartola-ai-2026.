import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Cartola AI 2026", 
    layout="wide", 
    page_icon="⚽",
    initial_sidebar_state="collapsed"
)

# --- FUNÇÃO DE CONEXÃO (BIGQUERY) ---
@st.cache_resource
def get_bq_client():
    """
    Conecta ao BigQuery usando as credenciais salvas nos Secrets do Streamlit Cloud.
    """
    try:
        # Pega o JSON que você salvou nos Secrets com aspas triplas
        info_chave = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
        credentials = service_account.Credentials.from_service_account_info(info_chave)
        return bigquery.Client(credentials=credentials, project=info_chave['project_id'])
    except Exception as e:
        st.error(f"Erro ao conectar no Google Cloud: {e}")
        return None

# --- CARREGAMENTO DOS DADOS ---
@st.cache_data(ttl=600) # Atualiza os dados a cada 10 minutos
def carregar_dados():
    client = get_bq_client()
    if not client:
        return pd.DataFrame(), pd.DataFrame()

    # Define o caminho da tabela (Projeto.Dataset)
    project_id = client.project
    dataset_id = "cartola_analytics" 

    try:
        # 1. Busca todo o histórico de pontos
        query_hist = f"""
            SELECT * FROM `{project_id}.{dataset_id}.historico` 
            ORDER BY timestamp ASC
        """
        df_hist = client.query(query_hist).to_dataframe()

        # 2. Busca apenas o comentário mais recente da IA
        query_corneta = f"""
            SELECT * FROM `{project_id}.{dataset_id}.comentarios_ia` 
            ORDER BY data DESC LIMIT 1
        """
        df_corneta = client.query(query_corneta).to_dataframe()
        
        return df_hist, df_corneta
        
    except Exception as e:
        # Esse erro acontece se o GitHub Action ainda não tiver rodado a primeira vez
        return pd.DataFrame(), pd.DataFrame()

# --- INTERFACE DO DASHBOARD ---
st.title("⚽ Cartola AI - Monitoramento da Liga")
st.caption("Dados atualizados automaticamente via GitHub Actions + BigQuery")

df_hist, df_corneta = carregar_dados()

if not df_hist.empty:
    
    # === SEÇÃO 1: A CORNETA DA IA ===
    if not df_corneta.empty:
        texto_ia = df_corneta['texto'].iloc[0]
        st.info(f"🤖 **O Especialista diz:** {texto_ia}")

    st.divider()

    # === SEÇÃO 2: MÉTRICAS (KPIs) ===
    # Filtra apenas a última coleta de dados baseada no tempo
    ultimo_ts = df_hist['timestamp'].max()
    ranking_atual = df_hist[df_hist['timestamp'] == ultimo_ts].sort_values(by='pontos', ascending=False)
    
    lider = ranking_atual.iloc[0]
    vice_lider = ranking_atual.iloc[1]
    vice_lanterna = ranking_atual.iloc[-2]
    lanterna = ranking_atual.iloc[-1]
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    col1.metric("🥇 Líder", lider['nome'], f"{lider['pontos']:.1f} pts")
    col2.metric("🥈 Vice-Líder", vice_lider['nome'], f"{vice_lider['pontos']:.1f} pts")
    col3.metric("📅 Atualizado em", ultimo_ts.strftime('%d/%m às %H:%M'))
    col4.metric("🥉 Vice-Lanterna", vice_lanterna['nome'], f"{vice_lanterna['pontos']:.1f} pts")
    col5.metric("🐌 Lanterna", lanterna['nome'], f"{lanterna['pontos']:.1f} pts")

    st.divider()

    # === SEÇÃO 3: GRÁFICOS E TABELAS ===
    tab_grafico, tab_tabela, tab_filtro = st.tabs(["📈 Evolução da Rodada", "📋 Tabela Completa", "📊 Filtro por Rodada"])

    with tab_grafico:
        st.subheader("Quem está subindo e quem está descendo?")
        # Transforma os dados: Linha do tempo no eixo X, Times nas linhas coloridas
        df_pivot = df_hist.pivot(index='timestamp', columns='nome', values='pontos')
        st.line_chart(df_pivot)

    with tab_tabela:
        st.subheader("Classificação Detalhada")
        # Mostra a tabela bonita, escondendo o índice numérico feio
        st.dataframe(
            ranking_atual[['nome', 'nome_cartola', 'pontos', 'patrimonio']], 
            use_container_width=True, 
            hide_index=True
        )
    
    with tab_filtro:
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
    # Mensagem de espera caso o banco esteja vazio
    st.warning("⚠️ Ainda não há dados disponíveis.")
    st.markdown("""
    **O que fazer:**
    1. Verifique se o seu **GitHub Action** rodou com sucesso (deu "Verde").
    2. Se rodou agora, aguarde uns instantes e clique no botão abaixo.
    """)
    if st.button("🔄 Tentar Recarregar"):
        st.rerun()