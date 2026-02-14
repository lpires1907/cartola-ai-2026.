import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import json
from google.oauth2 import service_account
from datetime import datetime
import altair as alt

# --- CONFIGURAÇÕES DE PÁGINA ---
st.set_page_config(page_title="Liga SAS Brasil 2026", page_icon="⚽", layout="wide")

# --- CONEXÃO BIGQUERY (BLINDADA) ---
def get_bq_client():
    creds = None
    project_id = None
    
    # 1. Tenta Secrets do Streamlit (Prioridade para Cloud)
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        try:
            secret_val = st.secrets["GCP_SERVICE_ACCOUNT"]
            # Se for string, converte para dict. Se já for dict (TOML), usa direto.
            if isinstance(secret_val, str):
                info = json.loads(secret_val)
            else:
                info = dict(secret_val) # Garante que é um dicionário
            
            creds = service_account.Credentials.from_service_account_info(info)
            project_id = info['project_id']
        except Exception as e:
            st.error(f"Erro ao ler Secrets: {e}")
            return None

    # 2. Tenta Variável de Ambiente (Fallback Local/Docker)
    elif os.getenv('GCP_SERVICE_ACCOUNT'):
        try:
            val = os.getenv('GCP_SERVICE_ACCOUNT')
            info = json.loads(val) if isinstance(val, str) else val
            creds = service_account.Credentials.from_service_account_info(info)
            project_id = info['project_id']
        except: pass

    # 3. Tenta Arquivo Local (Desenvolvimento)
    elif os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")

    if creds and project_id:
        return bigquery.Client(credentials=creds, project=project_id)
    
    return None

client = get_bq_client()
DATASET_ID = "cartola_analytics"

# --- HELPER: MÊS ATUAL ---
def get_coluna_mes_atual():
    mes = datetime.now().month
    mapa = {
        1: "pontos_jan_fev", 2: "pontos_jan_fev", 3: "pontos_marco", 4: "pontos_abril",
        5: "pontos_maio", 6: "pontos_jun_jul", 7: "pontos_jun_jul", 8: "pontos_agosto",
        9: "pontos_setembro", 10: "pontos_outubro", 11: "pontos_nov_dez", 12: "pontos_nov_dez"
    }
    col = mapa.get(mes, "pontos_jan_fev")
    nome = col.replace("pontos_", "").replace("_", "/").capitalize()
    return col, nome

@st.cache_data(ttl=300)
def load_data(query):
    if not client: return pd.DataFrame()
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        # st.error(f"Erro SQL: {e}") # Descomente para debug se necessário
        return pd.DataFrame()

# --- INTERFACE ---
st.title("🏆 Liga SAS Brasil 2026")

tab1, tab2, tab3 = st.tabs(["⚽ Painel Principal", "🏆 Mata-Mata & Copas", "📋 Escalações Detalhadas"])

# ==============================================================================
# ABA 1: PAINEL PRINCIPAL (Resgatado)
# ==============================================================================
with tab1:
    if client:
        # 1. Narrador no Topo
        df_ia = load_data(f"SELECT texto, tipo FROM `{client.project}.{DATASET_ID}.comentarios_ia` ORDER BY data DESC LIMIT 1") # nosec B608
        if not df_ia.empty:
            st.info(f"🎙️ **Narrador ({df_ia.iloc[0]['tipo']}):** {df_ia.iloc[0]['texto']}")

        # 2. Dados Gerais
        df_view = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.view_consolidada_times`") # nosec B608
        
        # 3. Dados da Última Rodada (Para o KPI de Rodada)
        q_last = f"""
        SELECT h.nome, h.pontos, h.rodada, h.tipo_dado 
        FROM `{client.project}.{DATASET_ID}.historico` h
        WHERE h.rodada = (SELECT MAX(rodada) FROM `{client.project}.{DATASET_ID}.historico`)
        ORDER BY h.pontos DESC
        """ # nosec B608
        df_rodada = load_data(q_last)

        st.divider()

        if not df_view.empty and not df_rodada.empty:
            col_mes_id, col_mes_nome = get_coluna_mes_atual()
            
            top_geral = df_view.sort_values('total_geral', ascending=False).head(5)
            top_rodada = df_rodada.head(2)
            
            # --- LINHA 1: KPIs ---
            c1, c2, c3, c4 = st.columns(4)
            
            # Geral
            lider = top_geral.iloc[0]
            c1.metric("🥇 Líder Geral", lider['nome'], f"{lider['total_geral']:.2f}")
            if len(top_geral) > 1:
                diff = lider['total_geral'] - top_geral.iloc[1]['total_geral']
                c1.caption(f"Vantagem: +{diff:.2f}")

            # Rodada
            r_lider = top_rodada.iloc[0]
            status_txt = "Ao Vivo" if r_lider['tipo_dado'] == 'PARCIAL' else "Fechada"
            c2.metric(f"⚽ Rodada {r_lider['rodada']} ({status_txt})", r_lider['nome'], f"{r_lider['pontos']:.2f}")
            
            # Mês
            if col_mes_id in df_view.columns:
                top_mes = df_view.sort_values(col_mes_id, ascending=False).head(1)
                if not top_mes.empty:
                    m_lider = top_mes.iloc[0]
                    c3.metric(f"📅 Mês {col_mes_nome}", m_lider['nome'], f"{m_lider[col_mes_id]:.2f}")
            
            # Patrimônio
            rico = df_view.sort_values('patrimonio_atual', ascending=False).iloc[0]
            c4.metric("💰 O Mais Rico", rico['nome'], f"C$ {rico['patrimonio_atual']:.2f}")

            st.divider()

            # --- LINHA 2: GRÁFICOS ---
            g1, g2 = st.columns(2)
            
            with g1:
                st.subheader("🏆 Top 5 Geral")
                chart_g = alt.Chart(top_geral).mark_bar().encode(
                    x=alt.X('total_geral', title='Pontos'),
                    y=alt.Y('nome', sort='-x', title=None),
                    color=alt.value('#f9c74f'),
                    tooltip=['nome', 'total_geral']
                )
                st.altair_chart(chart_g, use_container_width=True)

            with g2:
                st.subheader(f"📅 Top 5 {col_mes_nome}")
                if col_mes_id in df_view.columns:
                    top_m = df_view.sort_values(col_mes_id, ascending=False).head(5)
                    chart_m = alt.Chart(top_m).mark_bar().encode(
                        x=alt.X(col_mes_id, title='Pontos'),
                        y=alt.Y('nome', sort='-x', title=None),
                        color=alt.value('#90be6d'),
                        tooltip=['nome', col_mes_id]
                    )
                    st.altair_chart(chart_m, use_container_width=True)

            # --- TABELA DETALHADA ---
            with st.expander("📊 Ver Classificação Completa", expanded=True):
                cols = ['nome', 'nome_cartola', 'total_geral', 'media', 'maior_pontuacao', 'rodadas_jogadas', 'patrimonio_atual']
                # Garante que as colunas existem antes de exibir
                cols_finais = [c for c in cols if c in df_view.columns]
                st.dataframe(df_view[cols_finais].style.format(precision=2), use_container_width=True)

        else:
            st.warning("Aguardando dados... Se for a primeira execução, rode o coletor.")
    else:
        st.error("🔒 Erro de Autenticação: Verifique os Secrets (GCP_SERVICE_ACCOUNT).")

# ==============================================================================
# ABA 2: MATA-MATA (Visualização Melhorada)
# ==============================================================================
with tab2:
    st.header("🏆 Copas e Eliminatórias")
    if client:
        df_copa = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.copa_mata_mata` ORDER BY data_coleta DESC") # nosec B608
        
        if not df_copa.empty:
            copas = df_copa['nome_copa'].unique()
            sel_copa = st.selectbox("Selecione a Copa:", copas)
            df_c = df_copa[df_copa['nome_copa'] == sel_copa]
            
            fases = df_c['fase_copa'].unique()
            for fase in fases:
                with st.expander(f"📍 {fase}", expanded=True):
                    jogos = df_c[df_c['fase_copa'] == fase]
                    for _, j in jogos.iterrows():
                        # Exibe o Nome (já corrigido pelo coletor na tabela dimensão/copa)
                        n_a = j['time_a_nome']
                        n_b = j['time_b_nome']
                        
                        c1, c2, c3 = st.columns([3, 2, 3])
                        
                        # Destaca vencedor
                        win = j['vencedor']
                        bold_a = "**" if win and str(win) in str(j['time_a_slug']) else ""
                        bold_b = "**" if win and str(win) in str(j['time_b_slug']) else ""
                        
                        c1.markdown(f"<div style='text-align: right'>{bold_a}{n_a}{bold_a}</div>", unsafe_allow_html=True)
                        c2.markdown(f"<div style='text-align: center; background:#eee; border-radius:4px; color:#333'>{j['time_a_pontos']:.2f} x {j['time_b_pontos']:.2f}</div>", unsafe_allow_html=True)
                        c3.markdown(f"<div style='text-align: left'>{bold_b}{n_b}{bold_b}</div>", unsafe_allow_html=True)
        else:
            st.info("Nenhuma copa ativa encontrada.")

# ==============================================================================
# ABA 3: ESCALAÇÕES (Resgatada)
# ==============================================================================
with tab3:
    st.header("📋 Raio-X das Escalações")
    if client:
        q_rodadas = f"SELECT DISTINCT rodada FROM `{client.project}.{DATASET_ID}.times_escalacoes` ORDER BY rodada DESC" # nosec B608
        df_r = load_data(q_rodadas)
        
        if not df_r.empty:
            r_sel = st.selectbox("Rodada:", df_r['rodada'].tolist())
            
            q_esc = f"""
            SELECT liga_time_nome, atleta_posicao, atleta_apelido, pontos, is_capitao
            FROM `{client.project}.{DATASET_ID}.times_escalacoes`
            WHERE rodada = {r_sel}
            ORDER BY liga_time_nome, atleta_posicao
            """ # nosec B608
            df_esc = load_data(q_esc)
            
            times = df_esc['liga_time_nome'].unique()
            t_sel = st.multiselect("Filtrar Times:", times, default=times[:3]) # Default limita a 3 para não poluir
            
            if t_sel:
                df_final = df_esc[df_esc['liga_time_nome'].isin(t_sel)]
                st.dataframe(
                    df_final.style.format({'pontos': '{:.2f}'}).applymap(
                        lambda x: 'color: blue; font-weight: bold' if x is True else '', subset=['is_capitao']
                    ), 
                    use_container_width=True
                )
        else:
            st.warning("Sem dados de escalação.")
