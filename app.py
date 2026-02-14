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

# --- CONEXÃO BIGQUERY (COM SUPORTE A SECRETS) ---
def get_bq_client():
    # 1. Tenta Secrets do Streamlit (Produção)
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        try:
            info = json.loads(st.secrets["GCP_SERVICE_ACCOUNT"])
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=creds, project=info['project_id'])
        except: pass
    
    # 2. Tenta Variável de Ambiente (Local/Docker)
    env_sa = os.getenv('GCP_SERVICE_ACCOUNT')
    if env_sa:
        try:
            info = json.loads(env_sa)
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=creds, project=info['project_id'])
        except: pass

    # 3. Tenta Arquivo Local
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
        
    return None

client = get_bq_client()
DATASET_ID = "cartola_analytics"

# --- HELPER: MÊS ATUAL ---
def get_coluna_mes_atual():
    mes = datetime.now().month
    mapa_mes = {
        1: ("pontos_jan_fev", "Jan/Fev"), 2: ("pontos_jan_fev", "Jan/Fev"),
        3: ("pontos_marco", "Março"), 4: ("pontos_abril", "Abril"),
        5: ("pontos_maio", "Maio"), 6: ("pontos_jun_jul", "Jun/Jul"),
        7: ("pontos_jun_jul", "Jun/Jul"), 8: ("pontos_agosto", "Agosto"),
        9: ("pontos_setembro", "Setembro"), 10: ("pontos_outubro", "Outubro"),
        11: ("pontos_nov_dez", "Nov/Dez"), 12: ("pontos_nov_dez", "Nov/Dez")
    }
    return mapa_mes.get(mes, ("pontos_jan_fev", "Início"))

@st.cache_data(ttl=300)
def load_data(query):
    if not client: return pd.DataFrame()
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        return pd.DataFrame()

# --- INTERFACE ---
st.title("🏆 Liga SAS Brasil 2026")

tab1, tab2, tab3 = st.tabs(["⚽ Painel Principal", "🏆 Mata-Mata & Copas", "📋 Escalações Detalhadas"])

# ==============================================================================
# ABA 1: PAINEL PRINCIPAL (Resgatado com Gráficos e KPIs)
# ==============================================================================
with tab1:
    if client:
        # 1. Narrador (No topo, como solicitado)
        df_narrador = load_data(f"SELECT texto, tipo FROM `{client.project}.{DATASET_ID}.comentarios_ia` ORDER BY data DESC LIMIT 1") # nosec B608
        if not df_narrador.empty:
            st.info(f"🎙️ **{df_narrador.iloc[0]['tipo']}:** {df_narrador.iloc[0]['texto']}")

        # 2. Carrega Dados Principais
        df_view = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.view_consolidada_times`") # nosec B608
        
        # 3. Carrega Dados da Última Rodada (Para calcular campeão da rodada)
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
            
            # Ordenações
            top_geral = df_view.sort_values('total_geral', ascending=False).head(5)
            top_rodada = df_rodada.head(2)
            
            top_mes = pd.DataFrame()
            if col_mes_id in df_view.columns:
                top_mes = df_view.sort_values(col_mes_id, ascending=False).head(5)

            # --- LINHA 1: KPIS GERAIS ---
            c1, c2, c3, c4 = st.columns(4)
            
            # GERAL
            lider_geral = top_geral.iloc[0]
            c1.metric("🥇 Líder Geral", lider_geral['nome'], f"{lider_geral['total_geral']:.2f}")
            
            # RODADA
            lider_rodada = top_rodada.iloc[0]
            status_r = "Ao Vivo" if lider_rodada['tipo_dado'] == 'PARCIAL' else "Fechada"
            c2.metric(f"⚽ Rodada {lider_rodada['rodada']} ({status_r})", lider_rodada['nome'], f"{lider_rodada['pontos']:.2f}")

            # MENSAL
            if not top_mes.empty:
                lider_mes = top_mes.iloc[0]
                c3.metric(f"📅 Líder {col_mes_nome}", lider_mes['nome'], f"{lider_mes[col_mes_id]:.2f}")
            
            # PATRIMÔNIO
            rico = df_view.sort_values('patrimonio_atual', ascending=False).iloc[0]
            c4.metric("💰 O Mais Rico", rico['nome'], f"C$ {rico['patrimonio_atual']:.2f}")

            st.divider()

            # --- LINHA 2: GRÁFICOS (Resgatados) ---
            g1, g2 = st.columns(2)
            
            with g1:
                st.subheader("🏆 Top 5 - Campeonato Geral")
                chart_geral = alt.Chart(top_geral).mark_bar().encode(
                    x=alt.X('total_geral', title='Pontos Acumulados'),
                    y=alt.Y('nome', sort='-x', title=None),
                    color=alt.value('#f9c74f'),
                    tooltip=['nome', 'total_geral', 'nome_cartola']
                )
                st.altair_chart(chart_geral, use_container_width=True)

            with g2:
                st.subheader(f"📅 Top 5 - Mês de {col_mes_nome}")
                if not top_mes.empty:
                    chart_mes = alt.Chart(top_mes).mark_bar().encode(
                        x=alt.X(col_mes_id, title='Pontos no Mês'),
                        y=alt.Y('nome', sort='-x', title=None),
                        color=alt.value('#90be6d'),
                        tooltip=['nome', col_mes_id]
                    )
                    st.altair_chart(chart_mes, use_container_width=True)
                else:
                    st.info("Nenhum dado para o mês atual ainda.")

            # --- TABELA COMPLETA ---
            with st.expander("📊 Ver Classificação Completa", expanded=True):
                cols_view = ['nome', 'nome_cartola', 'total_geral', 'media', 'maior_pontuacao', 'rodadas_jogadas', 'patrimonio_atual']
                st.dataframe(
                    df_view[cols_view].style.format({
                        'total_geral': '{:.2f}', 'media': '{:.2f}', 
                        'maior_pontuacao': '{:.2f}', 'patrimonio_atual': '{:.2f}'
                    }), 
                    use_container_width=True
                )
        else:
            st.warning("Dados insuficientes para gerar o painel. Verifique se o coletor rodou.")
    else:
        st.error("Credenciais não configuradas. Verifique os Secrets do Streamlit.")

# ==============================================================================
# ABA 2: MATA-MATA (Com Correção de Nomes)
# ==============================================================================
with tab2:
    st.header("🏆 Copas e Eliminatórias")
    if client:
        df_copa = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.copa_mata_mata` ORDER BY data_coleta DESC") # nosec B608
        
        if not df_copa.empty:
            copas = df_copa['nome_copa'].unique()
            sel_copa = st.selectbox("Selecione o Torneio:", copas)
            df_c = df_copa[df_copa['nome_copa'] == sel_copa]
            
            # Ordem cronológica inversa das fases
            fases_display = df_c['fase_copa'].unique()
            
            for fase in fases_display:
                with st.expander(f"📍 {fase}", expanded=True):
                    jogos = df_c[df_c['fase_copa'] == fase]
                    for _, j in jogos.iterrows():
                        # Exibe nome. Se o nome for igual ao ID (erro), tenta mostrar o slug ou aviso
                        nome_a = j['time_a_nome']
                        nome_b = j['time_b_nome']
                        
                        col_a, col_placar, col_b = st.columns([3, 2, 3])
                        
                        # Formatação visual
                        win = j['vencedor']
                        # Verifica se o slug do vencedor está contido no slug do time (match parcial)
                        a_bold = "**" if win and str(win) in str(j['time_a_slug']) else ""
                        b_bold = "**" if win and str(win) in str(j['time_b_slug']) else ""
                        
                        col_a.markdown(f"<div style='text-align: right'>{a_bold}{nome_a}{a_bold}</div>", unsafe_allow_html=True)
                        col_placar.markdown(f"<div style='text-align: center; background-color: #eee; border-radius: 5px; color: black;'>{j['time_a_pontos']:.2f} x {j['time_b_pontos']:.2f}</div>", unsafe_allow_html=True)
                        col_b.markdown(f"<div style='text-align: left'>{b_bold}{nome_b}{b_bold}</div>", unsafe_allow_html=True)
        else:
            st.info("A tabela de copas está vazia. Aguardando processamento da próxima rodada.")

# ==============================================================================
# ABA 3: ESCALAÇÕES (Resgatada)
# ==============================================================================
with tab3:
    st.header("📋 Raio-X das Escalações")
    if client:
        # Filtro de Rodada
        q_r = f"SELECT DISTINCT rodada FROM `{client.project}.{DATASET_ID}.times_escalacoes` ORDER BY rodada DESC" # nosec B608
        df_r = load_data(q_r)
        
        if not df_r.empty:
            rodada_sel = st.selectbox("Selecione a Rodada:", df_r['rodada'].tolist())
            
            q_esc = f"""
            SELECT liga_time_nome, atleta_posicao, atleta_apelido, pontos, is_capitao
            FROM `{client.project}.{DATASET_ID}.times_escalacoes`
            WHERE rodada = {rodada_sel}
            ORDER BY liga_time_nome, atleta_posicao
            """ # nosec B608
            df_esc = load_data(q_esc)
            
            times = df_esc['liga_time_nome'].unique()
            time_sel = st.multiselect("Filtrar Times:", times, default=times)
            
            if time_sel:
                df_final = df_esc[df_esc['liga_time_nome'].isin(time_sel)]
                st.dataframe(
                    df_final.style.format({'pontos': '{:.2f}'}).applymap(
                        lambda x: 'font-weight: bold; color: blue' if x is True else '', subset=['is_capitao']
                    ),
                    use_container_width=True
                )
        else:
            st.warning("Nenhum dado de escalação encontrado.")
