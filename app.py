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

# --- ESTILOS CSS ---
st.markdown("""
    <style>
    .stMetric { background-color: #f8f9fa; padding: 15px; border-radius: 10px; border: 1px solid #e9ecef; }
    </style>
""", unsafe_allow_html=True)

# --- CONEXÃO BIGQUERY ---
@st.cache_resource
def get_bq_client():
    try:
        info = st.secrets["GCP_SERVICE_ACCOUNT"]
        return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(info), project=info['project_id'])
    except Exception as e:
        st.error(f"Erro de conexão com BigQuery: {e}")
        return None

# --- FUNÇÃO DE FALLBACK (PLANO B) ---
def gerar_ranking_provisorio(df_evo):
    """Gera um ranking na hora usando parciais se a View Oficial estiver vazia."""
    if df_evo is None or df_evo.empty: return pd.DataFrame()
    
    # Agrupa e soma
    df_temp = df_evo.groupby('nome')['pontos'].sum().reset_index()
    df_temp.columns = ['nome', 'total_geral']
    
    # Cria colunas fakes para o layout não quebrar
    df_temp['total_turno_1'] = df_temp['total_geral']
    df_temp['total_turno_2'] = 0
    df_temp['media_pontos'] = df_temp['total_geral']
    df_temp['mediana_pontos'] = df_temp['total_geral']
    df_temp['maior_pontuacao'] = df_temp['total_geral'] # Provisório
    
    return df_temp.sort_values(by='total_geral', ascending=False)

# --- CARREGAR DADOS ---
@st.cache_data(ttl=300)
def carregar_dados():
    client = get_bq_client()
    if not client: return None, None, None, None

    proj = client.project
    ds = "cartola_analytics"

    try:
        # 1. VIEW CONSOLIDADA (Oficial)
        # Se der erro ou vier vazia, usaremos o plano B
        try:
            df_cons = client.query(f"SELECT * FROM `{proj}.{ds}.view_consolidada_times` ORDER BY total_geral DESC").to_dataframe()
        except: df_cons = pd.DataFrame()

        # 2. HISTÓRICO COMPLETO (Evolução + Parciais)
        df_evo = client.query(f"""
            SELECT nome, rodada, pontos, tipo_dado 
            FROM `{proj}.{ds}.historico` 
            ORDER BY rodada ASC
        """).to_dataframe()

        # 3. ESCALAÇÕES DETALHADAS (Última rodada disponível)
        try:
            df_esc = client.query(f"""
                SELECT * FROM `{proj}.{ds}.times_escalacoes` 
                WHERE rodada = (SELECT MAX(rodada) FROM `{proj}.{ds}.times_escalacoes`)
            """).to_dataframe()
        except: df_esc = pd.DataFrame()

        # 4. CORNETA IA
        try:
            df_ia = client.query(f"""
                SELECT * FROM `{proj}.{ds}.comentarios_ia` 
                ORDER BY data DESC LIMIT 10
            """).to_dataframe()
        except: df_ia = pd.DataFrame()

        return df_cons, df_evo, df_esc, df_ia
    except Exception as e: 
        st.error(f"Erro geral ao carregar dados: {e}")
        return None, None, None, None

# --- INTERFACE PRINCIPAL ---
st.title("⚽ Cartola Analytics 2026")

df_cons, df_evo, df_esc, df_ia = carregar_dados()

# LÓGICA DE DADOS PROVISÓRIOS
usando_provisorio = False

if (df_cons is None or df_cons.empty):
    if (df_evo is not None and not df_evo.empty):
        df_cons = gerar_ranking_provisorio(df_evo)
        usando_provisorio = True
    else:
        st.warning("⚠️ Aguardando carga inicial de dados. O robô coletor deve rodar em breve.")
        if st.button("🔄 Tentar Recarregar"): st.rerun()
        st.stop()

# --- NARRADOR (IA) ---
txt_rodada = None
txt_geral = None

if not df_ia.empty:
    if 'tipo' in df_ia.columns:
        filt_rodada = df_ia[df_ia['tipo'] == 'RODADA']
        filt_geral = df_ia[df_ia['tipo'] == 'GERAL']
        if not filt_rodada.empty: txt_rodada = filt_rodada.iloc[0]['texto']
        if not filt_geral.empty: txt_geral = filt_geral.iloc[0]['texto']
    else:
        # Compatibilidade com versão antiga da tabela
        txt_rodada = df_ia.iloc[0]['texto']

# ALERTA SE FOR PARCIAL
if usando_provisorio:
    st.warning("🚧 Classificação baseada em **Parciais Ao Vivo** (Rodada ainda não fechou).")

# EXIBE NARRADOR
if txt_rodada:
    # Tenta descobrir se é parcial ou oficial pela última linha do histórico
    status_dados = "PARCIAL"
    if not df_evo.empty:
        status_dados = df_evo.iloc[-1]['tipo_dado']
        
    icon = "🔴" if status_dados == "PARCIAL" else "🟢"
    st.info(f"{icon} **Resumo da Rodada:** {txt_rodada}")

st.divider()

# --- ABAS ---
tab1, tab2, tab3 = st.tabs(["🏆 Classificação Geral", "📈 Evolução", "👕 Escalações"])

# === ABA 1: CLASSIFICAÇÃO ===
with tab1:
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
        
        if not usando_provisorio:
            st.metric("📊 Média do Líder", f"{lider['media_pontos']:.1f} pts/rodada")
            # Verifica se tem coluna maior_pontuacao antes de usar
            if 'maior_pontuacao' in df_cons.columns:
                recordista = df_cons.sort_values('maior_pontuacao', ascending=False).iloc[0]
                st.metric("🚀 Maior 'Mitada'", recordista['nome'], f"{recordista['maior_pontuacao']:.1f} pts")

    with col_tab:
        st.subheader("Tabela do Campeonato")
        cols_view = ['nome', 'total_geral']
        if not usando_provisorio:
            cols_view += ['total_turno_1', 'total_turno_2', 'media_pontos']
        
        st.dataframe(
            df_cons[cols_view],
            column_config={
                "nome": "Time",
                "total_geral": st.column_config.NumberColumn("Total", format="%.1f"),
                "total_turno_1": st.column_config.NumberColumn("1º Turno", format="%.1f"),
                "total_turno_2": st.column_config.NumberColumn("2º Turno", format="%.1f"),
                "media_pontos": st.column_config.NumberColumn("Média", format="%.1f"),
            },
            use_container_width=True,
            hide_index=True
        )

# === ABA 2: EVOLUÇÃO ===
with tab2:
    st.subheader("Corrida pelo Título (Acumulado)")
    if not df_evo.empty:
        df_pivot = df_evo.pivot_table(index='rodada', columns='nome', values='pontos', aggfunc='sum').fillna(0)
        df_acumulado = df_pivot.cumsum()
        st.line_chart(df_acumulado)
    else:
        st.info("Sem dados de evolução.")

# === ABA 3: ESCALAÇÕES (COM PROTEÇÃO CONTRA KEYERROR) ===
with tab3:
    st.subheader("Raio-X da Rodada")
    
    if not df_evo.empty:
        rodadas_disponiveis = sorted(df_evo['rodada'].unique(), reverse=True)
        rodada_sel = st.selectbox("Filtrar por Rodada:", rodadas_disponiveis)
        
        st.write(f"**Pontuação na Rodada {rodada_sel}:**")
        df_rodada_stats = df_evo[df_evo['rodada'] == rodada_sel].sort_values(by='pontos', ascending=False)
        
        # Mostra tabela simples (Ranking da Rodada)
        st.dataframe(
            df_rodada_stats[['nome', 'pontos', 'tipo_dado']].reset_index(drop=True).assign(Pos=lambda x: x.index+1).set_index('Pos'),
            use_container_width=True
        )
        
        st.divider()
        
        # Detalhes (Jogadores) - Só se a rodada selecionada tiver detalhes no banco
        rodada_detalhada_db = int(df_esc['rodada'].iloc[0]) if not df_esc.empty else -1
        
        if rodada_sel == rodada_detalhada_db and not df_esc.empty:
            st.subheader(f"Escalações Detalhadas (Rodada {rodada_sel})")
            time_sel = st.selectbox("Ver time:", sorted(df_esc['liga_time_nome'].unique()))
            
            df_time = df_esc[df_esc['liga_time_nome'] == time_sel].sort_values(by='pontos', ascending=False)
            
            # --- CRIAÇÃO SEGURA DA COLUNA CAPITÃO ---
            if 'is_capitao' in df_time.columns:
                df_time['C'] = df_time['is_capitao'].apply(lambda x: "©️" if x else "")
            else:
                df_time['C'] = ""

            # --- SELEÇÃO SEGURA DE COLUNAS ---
            # Lista de desejos
            cols_desejadas = ['C', 'atleta_posicao', 'atleta_apelido', 'atleta_clube', 'pontos']
            # Filtra apenas o que existe de verdade no DataFrame
            cols_finais = [c for c in cols_desejadas if c in df_time.columns]

            # Configuração Visual
            config_cols = {
                "C": "Capitão",
                "atleta_posicao": "Posição",
                "atleta_apelido": "Jogador",
                "atleta_clube": "Clube",
                "pontos": st.column_config.NumberColumn("Pts", format="%.1f")
            }

            st.dataframe(
                df_time[cols_finais], # <--- Protegido contra KeyError
                column_config=config_cols,
                use_container_width=True,
                hide_index=True
            )
        else:
            if df_esc.empty:
                st.info("⚠️ Ainda não há detalhes de escalação carregados no banco.")
            else:
                st.info(f"⚠️ Detalhes de jogadores disponíveis apenas para a última rodada carregada ({rodada_detalhada_db}).")
    else:
        st.info("Sem dados de rodadas ainda.")