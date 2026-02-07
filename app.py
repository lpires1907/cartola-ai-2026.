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

# --- CONEXÃO BQ (BLINDADA CONTRA TYPEERROR) ---
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
        
        # --- CORREÇÃO CRÍTICA: FORÇAR TIPOS NUMÉRICOS ---
        # Isso evita o TypeError na hora de fazer contas (subtração) nos KPIs
        cols_numericas = ['total_geral', 'pontos_turno_1', 'pontos_turno_2', 'maior_pontuacao', 'menor_pontuacao']
        
        # Garante que as colunas existem, senão cria com 0.0
        for col in cols_numericas:
            if col not in df_view.columns:
                df_view[col] = 0.0
            else:
                # Converte para numérico e preenche nulos com 0
                df_view[col] = pd.to_numeric(df_view[col], errors='coerce').fillna(0.0)

        # Também converte as colunas de meses dinamicamente
        cols_meses = [c for c in df_view.columns if 'pontos_' in c]
        for col in cols_meses:
             df_view[col] = pd.to_numeric(df_view[col], errors='coerce').fillna(0.0)
        # --------------------------------------------------

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
        # Se der erro no SQL, retorna estrutura vazia para não quebrar o app inteiro
        st.error(f"Erro ao carregar dados do BigQuery: {e}")
        return pd.DataFrame(), 0, "Erro", "total_geral"

# Carrega os dados
with st.spinner('Carregando dados do Cartola...'):
    df_view, rodada_atual, nome_mes_atual, col_mes_atual = load_data()

# Se o DataFrame estiver vazio (banco zerado), para aqui
if df_view.empty:
    st.warning("⚠️ Nenhum dado encontrado no BigQuery. Rode o coletor para popular a tabela.")
    st.stop()

# --- LÓGICA DE NEGÓCIO ---
is_segundo_turno = rodada_atual >= 19
coluna_turno = 'pontos_turno_2' if is_segundo_turno else 'pontos_turno_1'
nome_turno = "2º Turno" if is_segundo_turno else "1º Turno"

# Filtros e Ordenações
top_geral = df_view.sort_values('total_geral', ascending=False)
top_turno = df_view.sort_values(coluna_turno, ascending=False)
# Garante que não quebre se tiver vazio
top_mitada = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0] if not df_view.empty else None
top_zicada = df_view.sort_values('menor_pontuacao', ascending=True).iloc[0] if not df_view.empty else None

# --- CABEÇALHO ---
st.title(f"🏆 Cartola Analytics - Rodada {rodada_atual}")
st.markdown("---")

# --- DESTAQUES (KPIs) ---
c1, c2, c3, c4 = st.columns(4)

# BLINDAGEM: Verifica se tem times suficientes para Lider e Vice
tem_dados_suficientes = len(top_geral) >= 2

with c1:
    st.markdown("### 🥇 Geral")
    if tem_dados_suficientes:
        lider = top_geral.iloc[0]
        vice = top_geral.iloc[1]
        st.metric("Líder", lider['nome'], f"{lider['total_geral']:.1f}")
        # AQUI OCORRIA O ERRO: Agora garantimos que são floats na carga de dados
        delta_val = vice['total_geral'] - lider['total_geral']
        st.metric("Vice", vice['nome'], f"{vice['total_geral']:.1f}", delta=f"{delta_val:.1f}")
    elif len(top_geral) == 1:
        st.metric("Líder", top_geral.iloc[0]['nome'], f"{top_geral.iloc[0]['total_geral']:.1f}")

with c2:
    st.markdown(f"### 🥈 {nome_turno}")
    if tem_dados_suficientes:
        lider_t = top_turno.iloc[0]
        vice_t = top_turno.iloc[1]
        pts_lider = lider_t[coluna_turno]
        pts_vice = vice_t[coluna_turno]
        st.metric("Líder", lider_t['nome'], f"{pts_lider:.1f}")
        st.metric("Vice", vice_t['nome'], f"{pts_vice:.1f}", delta=f"{pts_vice - pts_lider:.1f}")

with c3:
    st.markdown("### 🚀 Mitada")
    if top_mitada is not None:
        st.metric("Maior Pontuação", top_mitada['nome'], f"{top_mitada['maior_pontuacao']:.1f}")

with c4:
    st.markdown("### 🐢 Zicada")
    if top_zicada is not None:
        st.metric("Menor Pontuação (Zica)", top_zicada['nome'], f"{top_zicada['menor_pontuacao']:.1f}", delta_color="inverse")

st.markdown("---")

# --- GRÁFICOS TOP 5 ---
st.subheader("📊 Classificação Top 5")
tab1, tab2, tab3 = st.tabs(["🌎 Geral", f"🔄 {nome_turno}", f"📅 Mensal ({nome_mes_atual})"])

def plot_top5(df, y_col, color_col, title):
    if df.empty: return None
    df_top = df.sort_values(y_col, ascending=False).head(5)
    fig = px.bar(
        df_top, x=y_col, y='nome', text=y_col, orientation='h',
        color=color_col, color_continuous_scale='Greens', title=title
    )
    fig.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False)
    fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
    return fig

with tab1: st.plotly_chart(plot_top5(df_view, 'total_geral', 'total_geral', "Top 5 Geral"), use_container_width=True)
with tab2: st.plotly_chart(plot_top5(df_view, coluna_turno, coluna_turno, f"Top 5 - {nome_turno}"), use_container_width=True)
with tab3: st.plotly_chart(plot_top5(df_view, col_mes_atual, col_mes_atual, f"Top 5 - {nome_mes_atual}"), use_container_width=True)

# --- TABELA COMPLETA ---
st.markdown("---")
with st.expander("📋 Ver Tabela Completa (Todos os Meses)", expanded=False):
    st.dataframe(
        df_view.style.format("{:.1f}", subset=df_view.select_dtypes(include='number').columns)
               .background_gradient(subset=['total_geral'], cmap='Greens'),
        use_container_width=True
    )

# --- RAIO-X (DETALHADO) ---
st.markdown("---")
st.subheader("🔬 Raio-X Detalhado (Escalações)")
filtro_rodada = st.selectbox("Escolha a Rodada:", sorted(range(1, rodada_atual + 1), reverse=True))

@st.cache_data
def get_escalacoes(rodada):
    # Query segura para o Bandit ignorar
    q = f""" 
        SELECT liga_time_nome as Time, atleta_apelido as Jogador, atleta_posicao as Posicao, 
               pontos as Pontos, is_capitao as Capitao
        FROM `cartola_analytics.times_escalacoes`
        WHERE rodada = {rodada}
        ORDER BY Time, Pontos
