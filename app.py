import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import plotly.express as px

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Liga SAS Brasil 2026", page_icon="⚽", layout="wide")

# CSS Ajustado para Placar da Copa
st.markdown("""
<style>
    .metric-card {background-color: #f0f2f6; padding: 15px; border-radius: 10px; text-align: center;}
    [data-testid="stMetricValue"] {font-size: 1.8rem !important;}
    
    /* Estilo do Card de Jogo da Copa */
    .match-card {
        background-color: white;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        padding: 15px;
        margin-bottom: 10px;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .match-score { font-size: 1.4rem; font-weight: bold; color: #333; }
    .match-team { font-weight: 500; font-size: 1rem; }
    .match-winner { color: #2e7d32; font-weight: bold; }
    .match-phase { font-size: 0.8rem; color: #666; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 5px;}
</style>
""", unsafe_allow_html=True)

# --- FUNÇÕES DE SEGURANÇA E CONEXÃO ---
def safe_get(value):
    if isinstance(value, pd.Series):
        if value.empty: return None
        return value.iloc[0]
    return value

@st.cache_resource
def get_bq_client():
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    try:
        if "GCP_SERVICE_ACCOUNT" not in st.secrets:
            st.error("❌ Secret 'GCP_SERVICE_ACCOUNT' não encontrado.")
            st.stop()
        service_account_info = st.secrets["GCP_SERVICE_ACCOUNT"]
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

# --- CARGAS DE DADOS ---
@st.cache_data(ttl=600)
def load_data_geral():
    try:
        # View Consolidada
        query = "SELECT * FROM `cartola_analytics.view_consolidada_times`"
        df = client.query(query).to_dataframe()
        df = df.loc[:, ~df.columns.duplicated()] # Remove duplicatas
        return df
    except: return pd.DataFrame()

@st.cache_data(ttl=600)
def load_metadados():
    try:
        q = """
            SELECT MAX(rodada) as rodada_atual, 
            (SELECT Mensal FROM `cartola_analytics.Rodada_Mensal` WHERE Rodada = (SELECT MAX(rodada) FROM `cartola_analytics.historico`)) as mes_atual
            FROM `cartola_analytics.historico`
        """
        df = client.query(q).to_dataframe()
        r = int(safe_get(df['rodada_atual'])) if not df.empty and pd.notnull(safe_get(df['rodada_atual'])) else 1
        m = safe_get(df['mes_atual']) if not df.empty and pd.notnull(safe_get(df['mes_atual'])) else "Jan Fev"
        return r, m
    except: return 1, "Jan Fev"

@st.cache_data(ttl=600)
def load_corneta(rodada):
    try:
        q = f"SELECT texto, tipo FROM `cartola_analytics.comentarios_ia` WHERE rodada = {rodada} ORDER BY data DESC" # nosec
        return client.query(q).to_dataframe()
    except: return pd.DataFrame()

# --- CARREGA DADOS DA COPA ---
@st.cache_data(ttl=600)
def load_copas():
    try:
        # Pega sempre a coleta mais recente de cada confronto
        # Ordenado por data_coleta DESC para que a copa mais recente apareça primeiro na lista
        q = """
            SELECT * EXCEPT(rank)
            FROM (
                SELECT *, 
                    ROW_NUMBER() OVER(PARTITION BY nome_copa, fase_copa, time_a_slug, time_b_slug ORDER BY data_coleta DESC) as rank
                FROM `cartola_analytics.copa_mata_mata`
            )
            WHERE rank = 1
            ORDER BY data_coleta DESC
        """
        return client.query(q).to_dataframe()
    except: return pd.DataFrame()

# --- FUNÇÃO AUXILIAR: IDENTIFICAR COPA ATIVA ---
def get_copa_default_index(lista_opcoes):
    """
    Tenta ler o arquivo copas.json para achar a ativa.
    Se não achar, retorna 0 (a primeira da lista, que é a mais recente).
    """
    try:
        if os.path.exists("copas.json"):
            with open("copas.json", "r") as f:
                configs = json.load(f)
                
            # Procura a primeira copa marcada como ativa
            nome_ativa = next((c['nome_visual'] for c in configs if c.get('ativa')), None)
            
            if nome_ativa and nome_ativa in lista_opcoes:
                return list(lista_opcoes).index(nome_ativa)
    except:
        pass
    
    return 0 # Default: A primeira da lista

# --- INICIALIZAÇÃO ---
with st.spinner('Carregando Liga SAS Brasil...'):
    df_view = load_data_geral()
    rodada_atual, nome_mes_atual = load_metadados()
    df_corneta = load_corneta(rodada_atual)
    df_copas = load_copas()

# --- MAPA DE MESES ---
map_mes = {
    'Jan Fev': 'pontos_jan_fev', 'Março': 'pontos_marco', 'Abril': 'pontos_abril',
    'Maio': 'pontos_maio', 'Jun Jul': 'pontos_jun_jul', 'Agosto': 'pontos_agosto',
    'Setembro': 'pontos_setembro', 'Outubro': 'pontos_outubro', 'Nov Dez': 'pontos_nov_dez'
}
col_mes_atual = map_mes.get(nome_mes_atual, 'total_geral')

# --- HEADER E NARRADOR ---
st.title(f"🏆 Cartola Analytics - Liga SAS Brasil 2026")
st.caption(f"Dados atualizados até a Rodada {rodada_atual}")

if not df_corneta.empty:
    df_rodada = df_corneta[df_corneta['tipo'] == 'RODADA']
    df_geral = df_corneta[df_corneta['tipo'] == 'GERAL']
    
    if not df_rodada.empty:
        st.markdown("### 🎙️ Narrador IA da Rodada")
        st.info(df_rodada.iloc[0]['texto'], icon="🎙️")
    
    if not df_geral.empty:
        st.markdown("### 🧠 Narrador IA da Temporada")
        st.success(df_geral.iloc[0]['texto'], icon="🧠")

st.markdown("---")

# --- NAVEGAÇÃO PRINCIPAL ---
aba_liga, aba_copa = st.tabs(["⚽ Liga Pontos Corridos", "🏆 Copas Mata-Mata"])

# ==============================================================================
# ABA 1: LIGA PONTOS CORRIDOS
# ==============================================================================
with aba_liga:
    if df_view.empty:
        st.warning("Sem dados da Liga.")
    else:
        # Tratamento de Nulos
        cols_num = ['total_geral', 'pontos_turno_1', 'pontos_turno_2', 'maior_pontuacao', 'menor_pontuacao']
        for c in cols_num: 
            if c in df_view.columns: df_view[c] = pd.to_numeric(df_view[c], errors='coerce').fillna(0.0)
        if col_mes_atual in df_view.columns: df_view[col_mes_atual] = pd.to_numeric(df_view[col_mes_atual], errors='coerce').fillna(0.0)

        # Ordenações
        is_2turno = rodada_atual >= 19
        col_turno = 'pontos_turno_2' if is_2turno else 'pontos_turno_1'
        nome_turno = "2º Turno" if is_2turno else "1º Turno"

        top_geral = df_view.sort_values('total_geral', ascending=False)
        top_turno = df_view.sort_values(col_turno, ascending=False)
        top_mes = df_view.sort_values(col_mes_atual, ascending=False)
        top_mitada = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
        
        df_zica = df_view[df_view['menor_pontuacao'] > 0]
        top_zica = df_zica.sort_values('menor_pontuacao', ascending=True).iloc[0] if not df_zica.empty else None

        # KPIs
        c1, c2, c3, c4, c5 = st.columns(5)
        
        with c1:
            st.markdown("##### 🥇 Geral")
            if len(top_geral) >= 2:
                st.metric("Líder", str(safe_get(top_geral.iloc[0]['nome'])), f"{float(safe_get(top_geral.iloc[0]['total_geral'])):.1f}", delta_color="off")
                st.metric("Vice", str(safe_get(top_geral.iloc[1]['nome'])), f"{float(safe_get(top_geral.iloc[1]['total_geral'])):.1f}", delta_color="off")
        
        with c2:
            st.markdown(f"##### 🥈 {nome_turno}")
            if len(top_turno) >= 2:
                st.metric("Líder", str(safe_get(top_turno.iloc[0]['nome'])), f"{float(safe_get(top_turno.iloc[0][col_turno])):.1f}", delta_color="off")
                st.metric("Vice", str(safe_get(top_turno.iloc[1]['nome'])), f"{float(safe_get(top_turno.iloc[1][col_turno])):.1f}", delta_color="off")

        with c3:
            st.markdown(f"##### 📅 {nome_mes_atual}")
            if len(top_mes) >= 2:
                st.metric("Líder", str(safe_get(top_mes.iloc[0]['nome'])), f"{float(safe_get(top_mes.iloc[0][col_mes_atual])):.1f}", delta_color="off")
                st.metric("Vice", str(safe_get(top_mes.iloc[1]['nome'])), f"{float(safe_get(top_mes.iloc[1][col_mes_atual])):.1f}", delta_color="off")

        with c4:
            st.markdown("##### 🚀 Mitada")
            st.metric("Maior Pontuação", str(safe_get(top_mitada['nome'])), f"{float(safe_get(top_mitada['maior_pontuacao'])):.1f} pts")

        with c5:
            st.markdown("##### 🐢 Zicada")
            if top_zica is not None:
                st.metric("Menor (>0)", str(safe_get(top_zica['nome'])), f"{float(safe_get(top_zica['menor_pontuacao'])):.1f} pts", delta_color="inverse")

        st.divider()

        # Gráficos
        st.subheader("📊 Classificação Top 5")
        t1, t2, t3 = st.tabs(["🌎 Geral", f"🔄 {nome_turno}", f"📅 Mês"])
        
        def plot_bar(df, col, title):
            tmp = df.sort_values(col, ascending=False).head(5).copy()
            tmp[col] = tmp[col].astype(float)
            fig = px.bar(tmp, x=col, y='nome', text=col, orientation='h', color=col, color_continuous_scale='Greens')
            fig.update_layout(yaxis={'categoryorder':'total ascending'}, showlegend=False, height=300)
            fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            return fig

        with t1: st.plotly_chart(plot_bar(df_view, 'total_geral', "Geral"), use_container_width=True)
        with t2: st.plotly_chart(plot_bar(df_view, col_turno, nome_turno), use_container_width=True)
        with t3: st.plotly_chart(plot_bar(df_view, col_mes_atual, nome_mes_atual), use_container_width=True)

        # Tabela
        with st.expander("📋 Tabela Completa"):
            st.dataframe(df_view.style.background_gradient(subset=['total_geral'], cmap='Greens'), use_container_width=True)

# ==============================================================================
# ABA 2: COPAS MATA-MATA
# ==============================================================================
with aba_copa:
    if df_copas.empty:
        st.info("🏆 Nenhuma Copa cadastrada ou iniciada ainda.")
    else:
        # Lógica para definir o Index Default
        lista_copas = df_copas['nome_copa'].unique()
        idx_default = get_copa_default_index(lista_copas)
        
        copa_selecionada = st.selectbox("Selecione a Copa:", lista_copas, index=idx_default)
        
        # Filtra dados da copa
        df_atual = df_copas[df_copas['nome_copa'] == copa_selecionada].copy()
        
        if df_atual.empty:
            st.warning("Dados indisponíveis para esta copa.")
        else:
            # Agrupar por fase
            fases = df_atual['fase_copa'].unique()
            
            for fase in fases:
                st.markdown(f"### ⚔️ {fase}")
                duelos = df_atual[df_atual['fase_copa'] == fase]
                
                # Grid de Cards
                cols = st.columns(3) # 3 jogos por linha
                for idx, (_, row) in enumerate(duelos.iterrows()):
                    with cols[idx % 3]:
                        # Define cores do vencedor
                        cor_a = "match-winner" if row['vencedor'] == row['time_a_slug'] else ""
                        cor_b = "match-winner" if row['vencedor'] == row['time_b_slug'] else ""
                        
                        st.markdown(f"""
                        <div class="match-card">
                            <div class="match-phase">{row['fase_copa']}</div>
                            <div style="display: flex; justify-content: space-between; align_items: center;">
                                <div style="flex: 1; text-align: left;">
                                    <div class="{cor_a} match-team">{row['time_a_nome']}</div>
                                </div>
                                <div class="match-score">{row['time_a_pontos']:.1f}</div>
                            </div>
                            <div style="margin: 5px 0; border-bottom: 1px solid #eee;"></div>
                            <div style="display: flex; justify-content: space-between; align_items: center;">
                                <div style="flex: 1; text-align: left;">
                                    <div class="{cor_b} match-team">{row['time_b_nome']}</div>
                                </div>
                                <div class="match-score">{row['time_b_pontos']:.1f}</div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
            
            st.caption(f"Última atualização: {df_atual['data_coleta'].max()}")
