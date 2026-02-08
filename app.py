import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import plotly.express as px

# --- CONFIGURAÇÃO VISUAL ---
st.set_page_config(page_title="Liga SAS Brasil 2026", page_icon="⚽", layout="wide")

# CSS para esconder índices e melhorar visual
st.markdown("""
<style>
    .metric-card {background-color: #f0f2f6; padding: 15px; border-radius: 10px; text-align: center;}
    [data-testid="stMetricValue"] {font-size: 1.8rem !important;}
</style>
""", unsafe_allow_html=True)

# --- FUNÇÃO AUXILIAR DE SEGURANÇA ---
def safe_get(value):
    """
    Garante que o valor retornado seja um escalar (número ou string única),
    mesmo que o Pandas retorne uma Series (lista) devido a duplicatas.
    """
    if isinstance(value, pd.Series):
        if value.empty:
            return None
        return value.iloc[0]
    return value

# --- CONEXÃO BQ (BLINDADA) ---
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

# --- CARGA DE DADOS PRINCIPAL ---
@st.cache_data(ttl=600)
def load_data():
    try:
        # 1. View Consolidada
        query_view = "SELECT * FROM `cartola_analytics.view_consolidada_times`"
        df_view = client.query(query_view).to_dataframe()
        
        # Blindagem contra duplicatas e tipos
        df_view = df_view.loc[:, ~df_view.columns.duplicated()]
        cols_numericas = ['total_geral', 'pontos_turno_1', 'pontos_turno_2', 'maior_pontuacao', 'menor_pontuacao']
        for col in cols_numericas:
            if col not in df_view.columns: df_view[col] = 0.0
            else: df_view[col] = pd.to_numeric(df_view[col], errors='coerce').fillna(0.0)

        cols_meses = [c for c in df_view.columns if 'pontos_' in c]
        for col in cols_meses:
             df_view[col] = pd.to_numeric(df_view[col], errors='coerce').fillna(0.0)

        # 2. Metadados
        query_meta = """
            SELECT MAX(rodada) as rodada_atual, 
                (SELECT Mensal FROM `cartola_analytics.Rodada_Mensal` 
                    WHERE Rodada = (SELECT MAX(rodada) FROM `cartola_analytics.historico`)) as mes_atual
            FROM `cartola_analytics.historico`
        """
        df_meta = client.query(query_meta).to_dataframe()
        
        val_rodada = safe_get(df_meta['rodada_atual']) if not df_meta.empty else 1
        rodada = int(val_rodada) if pd.notnull(val_rodada) else 1
        
        val_mes = safe_get(df_meta['mes_atual']) if not df_meta.empty else "Jan Fev"
        mes_raw = val_mes if pd.notnull(val_mes) else "Jan Fev"
        
        map_mes = {
            'Jan Fev': 'pontos_jan_fev', 'Março': 'pontos_marco', 'Abril': 'pontos_abril',
            'Maio': 'pontos_maio', 'Jun Jul': 'pontos_jun_jul', 'Agosto': 'pontos_agosto',
            'Setembro': 'pontos_setembro', 'Outubro': 'pontos_outubro', 'Nov Dez': 'pontos_nov_dez'
        }
        col_mes = map_mes.get(mes_raw, 'total_geral')

        return df_view, rodada, mes_raw, col_mes

    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame(), 0, "Erro", "total_geral"

# --- CARREGA O NARRADOR ---
@st.cache_data(ttl=600)
def load_corneta(rodada):
    try:
        q = f"""
            SELECT texto, tipo 
            FROM `cartola_analytics.comentarios_ia` 
            WHERE rodada = {rodada}
            ORDER BY data DESC
        """ # nosec
        df = client.query(q).to_dataframe()
        return df
    except:
        return pd.DataFrame()

# Carrega Dados
with st.spinner('Carregando dados da Liga SAS Brasil...'):
    df_view, rodada_atual, nome_mes_atual, col_mes_atual = load_data()
    df_corneta = load_corneta(rodada_atual)

if df_view.empty:
    st.warning("⚠️ Nenhum dado encontrado no BigQuery. Rode o coletor para popular a tabela.")
    st.stop()

# --- LÓGICA DE NEGÓCIO ---
is_segundo_turno = rodada_atual >= 19
coluna_turno = 'pontos_turno_2' if is_segundo_turno else 'pontos_turno_1'
nome_turno = "2º Turno" if is_segundo_turno else "1º Turno"

# Ordenações
top_geral = df_view.sort_values('total_geral', ascending=False)
top_turno = df_view.sort_values(coluna_turno, ascending=False)
top_mes = df_view.sort_values(col_mes_atual, ascending=False)
top_mitada = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0] if not df_view.empty else None

# Zicada corrigida (> 0)
df_zica_validos = df_view[df_view['menor_pontuacao'] > 0]
if not df_zica_validos.empty:
    top_zicada = df_zica_validos.sort_values('menor_pontuacao', ascending=True).iloc[0]
else:
    top_zicada = None

# --- CABEÇALHO ---
st.title(f"🏆 Cartola Analytics - Liga SAS Brasil 2026 - Rodada {rodada_atual}")

# --- BLOCOS DO NARRADOR (SEPARADOS) ---
if not df_corneta.empty:
    
    # Filtra os tipos
    df_rodada = df_corneta[df_corneta['tipo'] == 'RODADA']
    df_geral = df_corneta[df_corneta['tipo'] == 'GERAL']
    
    # Bloco 1: Narrador da Rodada (Azul/Info)
    if not df_rodada.empty:
        st.markdown("### 🎙️ Narrador IA da Rodada")
        # Pega o texto mais recente (caso tenha duplicatas, pega o primeiro)
        texto_rodada = df_rodada.iloc[0]['texto']
        st.info(texto_rodada, icon="🎙️")
        
    # Bloco 2: Narrador da Temporada (Verde/Success ou Cinza/Secondary)
    if not df_geral.empty:
        st.markdown("### 🧠 Narrador IA da Temporada")
        texto_geral = df_geral.iloc[0]['texto']
        st.success(texto_geral, icon="🧠")

st.markdown("---")

# --- DESTAQUES (KPIs) ---
c1, c2, c3, c4, c5 = st.columns(5)
tem_dados = len(top_geral) >= 2

# 1. GERAL
with c1:
    st.markdown("### 🥇 Geral")
    if tem_dados:
        lider = top_geral.iloc[0]
        vice = top_geral.iloc[1]
        st.metric(label="Líder", value=str(safe_get(lider['nome'])), 
                  delta=f"Total: {float(safe_get(lider['total_geral'])):.1f}", delta_color="off")
        st.metric(label="Vice", value=str(safe_get(vice['nome'])), 
                  delta=f"Total: {float(safe_get(vice['total_geral'])):.1f}", delta_color="off")
    elif len(top_geral) == 1:
        st.metric(label="Líder", value=str(safe_get(top_geral.iloc[0]['nome'])), 
                  delta=f"{float(safe_get(top_geral.iloc[0]['total_geral'])):.1f} pts", delta_color="off")

# 2. TURNO
with c2:
    st.markdown(f"### 🥈 {nome_turno}")
    if tem_dados:
        lider_t = top_turno.iloc[0]
        vice_t = top_turno.iloc[1]
        st.metric(label="Líder", value=str(safe_get(lider_t['nome'])), 
                  delta=f"Total: {float(safe_get(lider_t[coluna_turno])):.1f}", delta_color="off")
        st.metric(label="Vice", value=str(safe_get(vice_t['nome'])), 
                  delta=f"Total: {float(safe_get(vice_t[coluna_turno])):.1f}", delta_color="off")

# 3. MÊS
with c3:
    st.markdown(f"### 📅 {nome_mes_atual}")
    if tem_dados:
        lider_m = top_mes.iloc[0]
        vice_m = top_mes.iloc[1]
        st.metric(label="Líder", value=str(safe_get(lider_m['nome'])), 
                  delta=f"Total: {float(safe_get(lider_m[col_mes_atual])):.1f}", delta_color="off")
        st.metric(label="Vice", value=str(safe_get(vice_m['nome'])), 
                  delta=f"Total: {float(safe_get(vice_m[col_mes_atual])):.1f}", delta_color="off")

# 4. MITADA
with c4:
    st.markdown("### 🚀 Mitada")
    if top_mitada is not None:
        val_mitada = float(safe_get(top_mitada['maior_pontuacao']))
        st.metric(label="Maior Pontuação", value=str(safe_get(top_mitada['nome'])), 
                  delta=f"{val_mitada:.1f} pts")

# 5. ZICADA
with c5:
    st.markdown("### 🐢 Zicada")
    if top_zicada is not None:
        val_zica = float(safe_get(top_zicada['menor_pontuacao']))
        st.metric(label="Menor Pontuação (>0)", value=str(safe_get(top_zicada['nome'])), 
                  delta=f"{val_zica:.1f} pts", delta_color="inverse")

st.markdown("---")

# --- GRÁFICOS ---
st.subheader("📊 Classificação Top 5")
tab1, tab2, tab3 = st.tabs(["🌎 Geral", f"🔄 {nome_turno}", f"📅 Mensal ({nome_mes_atual})"])

def plot_top5(df, y_col, color_col, title):
    if df.empty: return None
    df_top = df.sort_values(y_col, ascending=False).head(5)
    df_top = df_top.copy()
    df_top[y_col] = df_top[y_col].apply(lambda x: float(safe_get(x)))
    df_top['nome'] = df_top['nome'].apply(lambda x: str(safe_get(x)))

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
    df_display = df_view.copy()
    df_display = df_display.loc[:, ~df_display.columns.duplicated()]
    
    try:
        import matplotlib
        st.dataframe(
            df_display.style.format("{:.1f}", subset=df_display.select_dtypes(include='number').columns)
                   .background_gradient(subset=['total_geral'], cmap='Greens'),
            use_container_width=True
        )
    except ImportError:
        st.dataframe(df_display, use_container_width=True)

# --- RAIO-X ---
st.markdown("---")
st.subheader("🔬 Raio-X Detalhado (Escalações)")
filtro_rodada = st.selectbox("Escolha a Rodada:", sorted(range(1, rodada_atual + 1), reverse=True))

@st.cache_data
def get_escalacoes(rodada):
    q = f"""
        SELECT liga_time_nome as Time, atleta_apelido as Jogador, atleta_posicao as Posicao, 
               pontos as Pontos, is_capitao as Capitao
        FROM `cartola_analytics.times_escalacoes`
        WHERE rodada = {rodada}
        ORDER BY Time, Pontos DESC
    """ # nosec
    return client.query(q).to_dataframe()

df_detalhe = get_escalacoes(filtro_rodada)

if not df_detalhe.empty:
    st.dataframe(
        df_detalhe.style.format({"Pontos": "{:.1f}"})
              .applymap(lambda x: "background-color: #d1e7dd; font-weight: bold" if x else "", subset=['Capitao']),
        use_container_width=True,
        hide_index=True
    )
else:
    st.warning("Nenhum dado encontrado para esta rodada.")
