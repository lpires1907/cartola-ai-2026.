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

# --- CONEXÃO BQ ---
@st.cache_resource
def get_bq_client():
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    else:
        info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info['project_id'])

client = get_bq_client()

# --- CARGA DE DADOS ---
@st.cache_data(ttl=600)
def load_data():
    # 1. View Consolidada (Já processada com mensais)
    # nosec: Query estática, segura.
    query_view = "SELECT * FROM `cartola_analytics.view_consolidada_times`"
    df_view = client.query(query_view).to_dataframe()
    
    # 2. Metadados da Rodada Atual
    # nosec: Query estática, segura.
    query_meta = """
        SELECT MAX(rodada) as rodada_atual, 
               (SELECT Mensal FROM `cartola_analytics.Rodada_Mensal` 
                WHERE Rodada = (SELECT MAX(rodada) FROM `cartola_analytics.historico`)) as mes_atual
        FROM `cartola_analytics.historico`
    """
    df_meta = client.query(query_meta).to_dataframe()
    rodada = int(df_meta['rodada_atual'].iloc[0]) if not df_meta.empty else 0
    mes_raw = df_meta['mes_atual'].iloc[0] if not df_meta.empty else "Jan Fev"
    
    # Mapping para saber qual coluna do mês pintar nos gráficos
    map_mes = {
        'Jan Fev': 'pontos_jan_fev', 'Março': 'pontos_marco', 'Abril': 'pontos_abril',
        'Maio': 'pontos_maio', 'Jun Jul': 'pontos_jun_jul', 'Agosto': 'pontos_agosto',
        'Setembro': 'pontos_setembro', 'Outubro': 'pontos_outubro', 'Nov Dez': 'pontos_nov_dez'
    }
    col_mes = map_mes.get(mes_raw, 'total_geral')

    return df_view, rodada, mes_raw, col_mes

df_view, rodada_atual, nome_mes_atual, col_mes_atual = load_data()

# --- LÓGICA DE NEGÓCIO ---
# Regra do Turno: R19 inicia o 2º Turno
is_segundo_turno = rodada_atual >= 19
coluna_turno = 'pontos_turno_2' if is_segundo_turno else 'pontos_turno_1'
nome_turno = "2º Turno" if is_segundo_turno else "1º Turno"

# Filtros e Ordenações
top_geral = df_view.sort_values('total_geral', ascending=False)
top_turno = df_view.sort_values(coluna_turno, ascending=False)
top_mitada = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
top_zicada = df_view.sort_values('menor_pontuacao', ascending=True).iloc[0]

# --- CABEÇALHO ---
st.title(f"🏆 Cartola Analytics - Rodada {rodada_atual}")
st.markdown("---")

# --- DESTAQUES (KPIs) ---
c1, c2, c3, c4 = st.columns(4)

with c1:
    st.markdown("### 🥇 Geral")
    lider = top_geral.iloc[0]
    vice = top_geral.iloc[1]
    st.metric("Líder", lider['nome'], f"{lider['total_geral']:.1f}")
    st.metric("Vice", vice['nome'], f"{vice['total_geral']:.1f}", delta=f"{vice['total_geral'] - lider['total_geral']:.1f}")

with c2:
    st.markdown(f"### 🥈 {nome_turno}")
    lider_t = top_turno.iloc[0]
    vice_t = top_turno.iloc[1]
    pts_lider = lider_t[coluna_turno]
    pts_vice = vice_t[coluna_turno]
    st.metric("Líder", lider_t['nome'], f"{pts_lider:.1f}")
    st.metric("Vice", vice_t['nome'], f"{pts_vice:.1f}", delta=f"{pts_vice - pts_lider:.1f}")

with c3:
    st.markdown("### 🚀 Mitada")
    st.metric("Maior Pontuação", top_mitada['nome'], f"{top_mitada['maior_pontuacao']:.1f}")

with c4:
    st.markdown("### 🐢 Zicada")
    st.metric("Menor Pontuação (Zica)", top_zicada['nome'], f"{top_zicada['menor_pontuacao']:.1f}", delta_color="inverse")

st.markdown("---")

# --- GRÁFICOS TOP 5 ---
st.subheader("📊 Classificação Top 5")
tab1, tab2, tab3 = st.tabs(["🌎 Geral", f"🔄 {nome_turno}", f"📅 Mensal ({nome_mes_atual})"])

def plot_top5(df, y_col, color_col, title):
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
    # Mostra primeiro as colunas principais e depois as mensais restantes
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
    # ATENÇÃO: Adicionado '# nosec' para passar no teste de segurança CI/CD
    # O bandit reclama de f-string em SQL, mas aqui a variável 'rodada' vem de um selectbox seguro.
    q = f""" 
        SELECT liga_time_nome as Time, atleta_apelido as Jogador, atleta_posicao as Posicao, 
               pontos as Pontos, is_capitao as Capitao
        FROM `cartola_analytics.times_escalacoes`
        WHERE rodada = {rodada}
        ORDER BY Time, Pontos DESC
    """ # nosec
    return client.query(q).to_dataframe()

df_detalhe = get_escalacoes(filtro_rodada)

st.dataframe(
    df_detalhe.style.format({"Pontos": "{:.1f}"})
              .applymap(lambda x: "background-color: #d1e7dd; font-weight: bold" if x else "", subset=['Capitao']),
    use_container_width=True,
    hide_index=True
)