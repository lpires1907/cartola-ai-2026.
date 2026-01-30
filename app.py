import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(
    page_title="Cartola Analytics 2026",
    layout="wide",
    page_icon="⚽",
    initial_sidebar_state="collapsed"
)

# --- ESTILOS CSS (Visual Moderno) ---
st.markdown("""
    <style>
    .stMetric { background-color: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #e9ecef; }
    .css-1v0mbdj.etr89bj1 { display: block; } /* Ajuste de imagens */
    </style>
""", unsafe_allow_html=True)

# --- CONEXÃO BIGQUERY ---
@st.cache_resource
def get_bq_client():
    try:
        info = st.secrets["GCP_SERVICE_ACCOUNT"]
        return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(info), project=info['project_id'])
    except Exception as e:
        st.error(f"Erro de conexão: {e}")
        return None

# --- CARREGAR DADOS ---
@st.cache_data(ttl=300)
def carregar_dados():
    client = get_bq_client()
    if not client: return None, None, None, None

    proj = client.project
    ds = "cartola_analytics"

    try:
        # 1. VIEW CONSOLIDADA (Classificação Geral)
        df_cons = client.query(f"SELECT * FROM `{proj}.{ds}.view_consolidada_times` ORDER BY total_geral DESC").to_dataframe()

        # 2. HISTÓRICO COMPLETO (Para gráfico de evolução)
        df_evo = client.query(f"""
            SELECT nome, rodada, pontos, tipo_dado 
            FROM `{proj}.{ds}.historico` 
            ORDER BY rodada ASC
        """).to_dataframe()

        # 3. ESCALAÇÕES DETALHADAS (Da última rodada disponível)
        df_esc = client.query(f"""
            SELECT * FROM `{proj}.{ds}.times_escalacoes` 
            WHERE rodada = (SELECT MAX(rodada) FROM `{proj}.{ds}.times_escalacoes`)
        """).to_dataframe()

        # 4. CORNETA (Busca os 10 últimos para filtrar por tipo)
        df_ia = client.query(f"""
            SELECT * FROM `{proj}.{ds}.comentarios_ia` 
            ORDER BY data DESC LIMIT 10
        """).to_dataframe()

        return df_cons, df_evo, df_esc, df_ia
    except Exception: 
        return None, None, None, None

# --- INTERFACE PRINCIPAL ---
st.title("⚽ Cartola Analytics 2026")

df_cons, df_evo, df_esc, df_ia = carregar_dados()

# Tratamento para banco vazio
if df_cons is None or df_cons.empty:
    st.warning("⚠️ Aguardando carga inicial de dados. O robô coletor deve rodar em breve.")
    if st.button("🔄 Tentar Recarregar"): st.rerun()
    st.stop()

# --- SEPARAÇÃO DOS COMENTÁRIOS DA IA ---
txt_rodada = None
txt_geral = None

if not df_ia.empty:
    # Verifica se a tabela nova já tem a coluna 'tipo'
    if 'tipo' in df_ia.columns:
        # Pega o mais recente de cada tipo
        filt_rodada = df_ia[df_ia['tipo'] == 'RODADA']
        filt_geral = df_ia[df_ia['tipo'] == 'GERAL']
        
        if not filt_rodada.empty: txt_rodada = filt_rodada.iloc[0]['texto']
        if not filt_geral.empty: txt_geral = filt_geral.iloc[0]['texto']
    else:
        # Fallback para dados antigos (antes da atualização)
        txt_rodada = df_ia.iloc[0]['texto']

# 1. EXIBE NARRADOR DA RODADA (Destaque no topo)
if txt_rodada:
    status_dados = df_evo.iloc[-1]['tipo_dado'] # Pega status da última linha carregada
    icon = "🔴" if status_dados == "PARCIAL" else "🟢"
    st.info(f"{icon} **Resumo da Rodada:** {txt_rodada}")

st.divider()

# --- ABAS DE CONTEÚDO ---
tab1, tab2, tab3 = st.tabs(["🏆 Classificação Geral", "📈 Evolução", "👕 Escalações"])

# === ABA 1: VIEW CONSOLIDADA ===
with tab1:
    # 2. EXIBE ANALISTA GERAL (Box diferenciado dentro da classificação)
    if txt_geral:
        st.markdown(f"""
        <div style="background-color:#f0f8ff; padding:15px; border-radius:10px; margin-bottom:20px; border-left:5px solid #007bff;">
            <h4 style="margin-top:0; color: #007bff;">🧠 Análise de Temporada (IA)</h4>
            <p style="font-style:italic; margin-bottom:0;">"{txt_geral}"</p>
        </div>
        """, unsafe_allow_html=True)

    col_kpi, col_tab = st.columns([1, 2])
    
    with col_kpi:
        lider = df_cons.iloc[0]
        st.subheader("Destaques")
        st.metric("🥇 Líder Geral", lider['nome'], f"{lider['total_geral']:.1f} pts")
        st.metric("📊 Média do Líder", f"{lider['media_pontos']:.1f} pts/rodada")
        
        recordista = df_cons.sort_values('maior_pontuacao', ascending=False).iloc[0]
        st.metric("🚀 Maior 'Mitada'", recordista['nome'], f"{recordista['maior_pontuacao']:.1f} pts")

    with col_tab:
        st.subheader("Tabela do Campeonato")
        cols_view = ['nome', 'total_geral', 'total_turno_1', 'total_turno_2', 'media_pontos', 'mediana_pontos']
        
        st.dataframe(
            df_cons[cols_view],
            column_config={
                "nome": "Time",
                "total_geral": st.column_config.NumberColumn("Total", format="%.1f"),
                "total_turno_1": st.column_config.NumberColumn("1º Turno", format="%.1f"),
                "total_turno_2": st.column_config.NumberColumn("2º Turno", format="%.1f"),
                "media_pontos": st.column_config.NumberColumn("Média", format="%.1f"),
                "mediana_pontos": st.column_config.NumberColumn("Mediana", format="%.1f"),
            },
            use_container_width=True,
            hide_index=True
        )

# === ABA 2: EVOLUÇÃO ===
with tab2:
    st.subheader("Corrida pelo Título (Acumulado)")
    # Pivotar e Acumular
    df_pivot = df_evo.pivot_table(index='rodada', columns='nome', values='pontos', aggfunc='sum').fillna(0)
    df_acumulado = df_pivot.cumsum()
    st.line_chart(df_acumulado)

# === ABA 3: ESCALAÇÕES ===
with tab3:
    st.subheader("Raio-X da Rodada")
    
    rodadas_disponiveis = sorted(df_evo['rodada'].unique(), reverse=True)
    if rodadas_disponiveis:
        rodada_sel = st.selectbox("Filtrar por Rodada:", rodadas_disponiveis)
        
        # Tabela Simples da Rodada
        st.write(f"**Pontuação na Rodada {rodada_sel}:**")
        df_rodada_stats = df_evo[df_evo['rodada'] == rodada_sel].sort_values(by='pontos', ascending=False)
        st.dataframe(
            df_rodada_stats[['nome', 'pontos', 'tipo_dado']].reset_index(drop=True).assign(Pos=lambda x: x.index+1).set_index('Pos'),
            use_container_width=True
        )
        
        st.divider()
        
        # Detalhes (Jogadores e Capitão)
        # Só mostra se tiver dados detalhados para aquela rodada
        rodada_detalhada_db = int(df_esc['rodada'].iloc[0]) if not df_esc.empty else -1
        
        if rodada_sel == rodada_detalhada_db:
            st.subheader(f"Escalações Detalhadas (Rodada {rodada_sel})")
            time_sel = st.selectbox("Ver time:", sorted(df_esc['liga_time_nome'].unique()))
            
            df_time = df_esc[df_esc['liga_time_nome'] == time_sel].sort_values(by='pontos', ascending=False)
            
            # Coluna de Capitão Visual
            df_time['C'] = df_time['is_capitao'].apply(lambda x: "©️" if x else "")
            
            st.dataframe(
                df_time[['C', 'atleta_posicao', 'atleta_apelido', 'atleta_clube', 'pontos']],
                column_config={
                    "C": "Capitão",
                    "atleta_posicao": "Posição",
                    "atleta_apelido": "Jogador",
                    "atleta_clube": "Clube",
                    "pontos": st.column_config.NumberColumn("Pts", format="%.1f")
                },
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("⚠️ Detalhes de jogadores disponíveis apenas para a última rodada carregada.")
    else:
        st.info("Sem dados de rodadas ainda.")