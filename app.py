import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Cartola Analytics 2026",
    layout="wide",
    page_icon="⚽",
    initial_sidebar_state="expanded"
)

# --- ESTILOS CSS ---
st.markdown("""
    <style>
    .big-font { font-size:20px !important; }
    .stMetric { background-color: #f0f2f6; padding: 10px; border-radius: 5px; }
    </style>
""", unsafe_allow_html=True)

# --- CONEXÃO BIGQUERY ---
@st.cache_resource
def get_bq_client():
    try:
        info = st.secrets["GCP_SERVICE_ACCOUNT"]
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info['project_id'])
    except Exception as e:
        st.error(f"Erro de Conexão: {e}")
        return None

# --- CARREGAR DADOS ---
@st.cache_data(ttl=300) # Cache de 5 minutos
def carregar_dados_gerais():
    client = get_bq_client()
    if not client: return None, None, None

    project_id = client.project
    dataset = "cartola_analytics"

    try:
        # 1. RANKING ATUAL (Pega a última rodada disponível)
        query_rank = f"""
            SELECT * FROM `{project_id}.{dataset}.historico`
            WHERE rodada = (SELECT MAX(rodada) FROM `{project_id}.{dataset}.historico`)
            ORDER BY pontos DESC
        """
        df_rank = client.query(query_rank).to_dataframe()

        # 2. ESCALAÇÕES (Pega dados da última rodada)
        query_esc = f"""
            SELECT * FROM `{project_id}.{dataset}.times_escalacoes`
            WHERE rodada = (SELECT MAX(rodada) FROM `{project_id}.{dataset}.times_escalacoes`)
        """
        df_esc = client.query(query_esc).to_dataframe()

        # 3. CORNETA IA (Último comentário)
        query_ia = f"""
            SELECT * FROM `{project_id}.{dataset}.comentarios_ia`
            ORDER BY data DESC LIMIT 1
        """
        df_ia = client.query(query_ia).to_dataframe()

        return df_rank, df_esc, df_ia

    except Exception as e:
        # Retorna None para tratarmos o erro na interface sem crashar
        return None, None, None

# --- INTERFACE ---
st.title("⚽ Cartola Analytics 2026")

df_rank, df_esc, df_ia = carregar_dados_gerais()

# VERIFICAÇÃO DE DADOS VAZIOS
if df_rank is None or df_rank.empty:
    st.warning("⚠️ O Banco de Dados parece estar vazio ou inacessível.")
    st.info("💡 **Dica:** Se você acabou de resetar o banco, aguarde a execução do 'Coletor' no GitHub Actions.")
    
    if st.button("🔄 Tentar Recarregar"):
        st.cache_data.clear()
        st.rerun()
    st.stop() # Para a execução aqui se não tiver dados

# --- DADOS CARREGADOS COM SUCESSO ---

# Recupera infos da rodada atual
rodada_atual = df_rank['rodada'].iloc[0]
status_dados = df_rank['tipo_dado'].iloc[0] # OFICIAL ou PARCIAL
status_cor = "🟢" if status_dados == "OFICIAL" else "🔴"

st.caption(f"📅 Dados da **Rodada {rodada_atual}** • Status: {status_cor} **{status_dados}**")

# --- ÁREA DA IA (CORNETA) ---
if not df_ia.empty:
    with st.chat_message("assistant", avatar="🤖"):
        st.write(f"**Comentário do Especialista:** {df_ia['texto'].iloc[0]}")

st.divider()

# --- ABAS DE CONTEÚDO ---
tab1, tab2, tab3 = st.tabs(["🏆 Classificação", "👕 Escalações", "📊 Estatísticas"])

# === ABA 1: CLASSIFICAÇÃO ===
with tab1:
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Tabela da Liga")
        
        # Prepara tabela bonita
        df_display = df_rank[['nome', 'pontos', 'patrimonio', 'nome_cartola']].copy()
        df_display = df_display.reset_index(drop=True)
        df_display.index += 1 # Começar ranking do 1
        
        st.dataframe(
            df_display,
            column_config={
                "nome": "Time",
                "pontos": st.column_config.NumberColumn("Pontos", format="%.1f"),
                "patrimonio": st.column_config.NumberColumn("C$ Patrimônio", format="C$ %.2f"),
                "nome_cartola": "Cartoleiro"
            },
            use_container_width=True
        )

    with col2:
        st.subheader("Destaques")
        lider = df_rank.iloc[0]
        lanterna = df_rank.iloc[-1]
        
        st.metric("🥇 Líder", lider['nome'], f"{lider['pontos']:.1f} pts")
        st.metric("💰 Mais Rico", 
                  df_rank.sort_values('patrimonio', ascending=False).iloc[0]['nome'],
                  f"C$ {df_rank['patrimonio'].max():.2f}")
        st.metric("🐌 Lanterna", lanterna['nome'], f"{lanterna['pontos']:.1f} pts")

# === ABA 2: ESCALAÇÕES ===
with tab2:
    st.subheader("Raio-X dos Times")
    
    if df_esc is not None and not df_esc.empty:
        times_disponiveis = sorted(df_esc['liga_time_nome'].unique())
        time_selecionado = st.selectbox("Selecione um time para ver a escalação:", times_disponiveis)
        
        # Filtra escalação do time
        df_time = df_esc[df_esc['liga_time_nome'] == time_selecionado].sort_values(by='pontos', ascending=False)
        
        # Exibe escalação
        st.dataframe(
            df_time[['atleta_posicao', 'atleta_apelido', 'atleta_clube', 'pontos']],
            column_config={
                "atleta_posicao": "Posição",
                "atleta_apelido": "Jogador",
                "atleta_clube": "Clube Real",
                "pontos": st.column_config.NumberColumn("Pontos", format="%.1f")
            },
            use_container_width=True,
            hide_index=True
        )
        
        total_time = df_time['pontos'].sum()
        st.metric(f"Total {time_selecionado}", f"{total_time:.1f} pts")
    else:
        st.info("Nenhuma escalação detalhada encontrada para esta rodada.")

# === ABA 3: ESTATÍSTICAS ===
with tab3:
    st.subheader("Comparação de Patrimônio")
    st.bar_chart(df_rank.set_index('nome')['patrimonio'])