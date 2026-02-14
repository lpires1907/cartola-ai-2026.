import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import json
from google.oauth2 import service_account
from datetime import datetime
import altair as alt

# --- CONFIGURAÇÕES DE PÁGINA ---
st.set_page_config(page_title="Cartola SAS Analytics 2026", page_icon="⚽", layout="wide")

# --- CONEXÃO BIGQUERY ---
def get_bq_client():
    gcp_info = os.getenv('GCP_SERVICE_ACCOUNT')
    if gcp_info:
        try:
            info = json.loads(gcp_info) if isinstance(gcp_info, str) else gcp_info
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=creds, project=info['project_id'])
        except Exception: pass
    
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    return None

client = get_bq_client()
DATASET_ID = "cartola_analytics"

# --- HELPER: MÊS ATUAL ---
def get_coluna_mes_atual():
    mes = datetime.now().month
    if mes <= 2: return "pontos_jan_fev", "Jan/Fev"
    if mes == 3: return "pontos_marco", "Março"
    if mes == 4: return "pontos_abril", "Abril"
    if mes == 5: return "pontos_maio", "Maio"
    if mes <= 7: return "pontos_jun_jul", "Jun/Jul"
    if mes == 8: return "pontos_agosto", "Agosto"
    if mes == 9: return "pontos_setembro", "Setembro"
    if mes == 10: return "pontos_outubro", "Outubro"
    return "pontos_nov_dez", "Nov/Dez"

@st.cache_data(ttl=300)
def load_data(query):
    if not client: return pd.DataFrame()
    try:
        return client.query(query).to_dataframe()
    except: return pd.DataFrame()

# --- INTERFACE ---
st.title("🏆 Liga SAS Brasil 2026")

tab1, tab2, tab3 = st.tabs(["⚽ Painel Principal", "🏆 Mata-Mata & Copas", "📋 Escalações Detalhadas"])

# ==============================================================================
# ABA 1: PAINEL PRINCIPAL (Resgatado)
# ==============================================================================
with tab1:
    if client:
        # 1. Carrega Dados Principais
        df_view = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.view_consolidada_times`") # nosec B608
        
        # 2. Carrega Dados da Última Rodada (Para calcular campeão da rodada)
        q_last = f"""
        SELECT h.nome, h.pontos, h.rodada, h.tipo_dado 
        FROM `{client.project}.{DATASET_ID}.historico` h
        WHERE h.rodada = (SELECT MAX(rodada) FROM `{client.project}.{DATASET_ID}.historico`)
        ORDER BY h.pontos DESC
        """ # nosec B608
        df_rodada = load_data(q_last)

        # 3. Narrador (Destaque no Topo)
        df_narrador = load_data(f"SELECT texto, tipo FROM `{client.project}.{DATASET_ID}.comentarios_ia` ORDER BY data DESC LIMIT 1") # nosec B608
        if not df_narrador.empty:
            with st.chat_message("assistant", avatar="🎤"):
                st.write(f"**Narrador ({df_narrador.iloc[0]['tipo']}):** {df_narrador.iloc[0]['texto']}")

        st.divider()

        # --- LINHA 1: KPIS GERAIS ---
        if not df_view.empty and not df_rodada.empty:
            col_mes_id, col_mes_nome = get_coluna_mes_atual()
            
            # Ordenações
            top_geral = df_view.sort_values('total_geral', ascending=False).head(2)
            top_rodada = df_rodada.head(2)
            top_mes = pd.DataFrame()
            if col_mes_id in df_view.columns:
                top_mes = df_view.sort_values(col_mes_id, ascending=False).head(2)

            # Exibição
            c1, c2, c3 = st.columns(3)
            
            # GERAL
            c1.markdown("### 🥇 Geral")
            if len(top_geral) > 0:
                c1.metric("Líder", top_geral.iloc[0]['nome'], f"{top_geral.iloc[0]['total_geral']:.2f}")
                if len(top_geral) > 1:
                    c1.caption(f"🥈 Vice: {top_geral.iloc[1]['nome']} (-{(top_geral.iloc[0]['total_geral'] - top_geral.iloc[1]['total_geral']):.2f})")

            # RODADA
            c2.markdown(f"### ⚽ Rodada {df_rodada.iloc[0]['rodada']}")
            if len(top_rodada) > 0:
                status = "(Parcial)" if df_rodada.iloc[0]['tipo_dado'] == 'PARCIAL' else "(Fechada)"
                c2.metric(f"Mito {status}", top_rodada.iloc[0]['nome'], f"{top_rodada.iloc[0]['pontos']:.2f}")
                if len(top_rodada) > 1:
                    c2.caption(f"🥈 Vice: {top_rodada.iloc[1]['nome']}")

            # MENSAL
            c3.markdown(f"### 📅 Mês ({col_mes_nome})")
            if not top_mes.empty and len(top_mes) > 0:
                c3.metric("Líder Mensal", top_mes.iloc[0]['nome'], f"{top_mes.iloc[0][col_mes_id]:.2f}")
                if len(top_mes) > 1:
                    c3.caption(f"🥈 Vice: {top_mes.iloc[1]['nome']}")
            else:
                c3.info("Mês começando...")

            st.divider()

            # --- LINHA 2: GRÁFICOS (Resgatados) ---
            g1, g2 = st.columns(2)
            
            with g1:
                st.subheader("🏆 Top 5 Geral")
                chart_geral = alt.Chart(top_geral.head(5)).mark_bar().encode(
                    x=alt.X('total_geral', title='Pontos'),
                    y=alt.Y('nome', sort='-x', title=None),
                    color=alt.value('#f9c74f'),
                    tooltip=['nome', 'total_geral']
                )
                st.altair_chart(chart_geral, use_container_width=True)

            with g2:
                st.subheader(f"📅 Top 5 {col_mes_nome}")
                if not top_mes.empty:
                    chart_mes = alt.Chart(top_mes.head(5)).mark_bar().encode(
                        x=alt.X(col_mes_id, title='Pontos'),
                        y=alt.Y('nome', sort='-x', title=None),
                        color=alt.value('#90be6d'),
                        tooltip=['nome', col_mes_id]
                    )
                    st.altair_chart(chart_mes, use_container_width=True)
                else:
                    st.write("Sem dados mensais.")

            # --- TABELA COMPLETA ---
            st.subheader("📊 Classificação Completa")
            cols = ['nome', 'nome_cartola', 'total_geral', 'media', 'maior_pontuacao', 'patrimonio_atual']
            st.dataframe(df_view[cols], use_container_width=True, hide_index=True)

    else:
        st.error("Configure as credenciais.")

# ==============================================================================
# ABA 2: MATA-MATA (Com Nomes Corrigidos)
# ==============================================================================
with tab2:
    st.header("🏆 Copas e Eliminatórias")
    if client:
        df_copa = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.copa_mata_mata` ORDER BY data_coleta DESC") # nosec B608
        
        if not df_copa.empty:
            copas = df_copa['nome_copa'].unique()
            sel_copa = st.selectbox("Escolha a Competição:", copas)
            df_c = df_copa[df_copa['nome_copa'] == sel_copa]
            
            fases_ordem = ["F", "S", "Q", "O", "1", "2"] # Ordem visual de importância
            
            # Organiza a exibição das fases
            for fase_code in fases_ordem:
                # Filtra visualmente pelo nome da fase mapeado ou pelo código se necessário
                jogos = df_c[df_c['fase_copa'].str.contains(fase_code) | df_c['fase_copa'].isin(['Final', 'Semifinal', 'Quartas de Final', 'Oitavas de Final'])]
                
                # Se encontrar jogos dessa fase na tabela
                fase_nome_display = jogos['fase_copa'].iloc[0] if not jogos.empty else None
                
                # Agrupa por fase real do banco para não perder nada
                for fase_real in df_c['fase_copa'].unique():
                    jogos_fase = df_c[df_c['fase_copa'] == fase_real]
                    
                    with st.expander(f"📍 {fase_real} (Rodada {jogos_fase.iloc[0]['rodada_real']})", expanded=True):
                        for _, j in jogos_fase.iterrows():
                            # Layout de Placar
                            c_a, c_placar, c_b = st.columns([4, 2, 4])
                            
                            # Tenta mostrar nome, se falhar mostra slug (Resiliência)
                            nome_a = j['time_a_nome'] if j['time_a_nome'] else j['time_a_slug']
                            nome_b = j['time_b_nome'] if j['time_b_nome'] else j['time_b_slug']
                            
                            # Destaque para o vencedor
                            win = j['vencedor']
                            style_a = "**" if win and win in str(j['time_a_slug']) else ""
                            style_b = "**" if win and win in str(j['time_b_slug']) else ""
                            
                            c_a.markdown(f"<div style='text-align: right'>{style_a}{nome_a}{style_a}</div>", unsafe_allow_html=True)
                            c_placar.markdown(f"<div style='text-align: center; background-color: #f0f2f6; border-radius: 5px;'>{j['time_a_pontos']:.2f} x {j['time_b_pontos']:.2f}</div>", unsafe_allow_html=True)
                            c_b.markdown(f"<div style='text-align: left'>{style_b}{nome_b}{style_b}</div>", unsafe_allow_html=True)
                    break # Evita duplicar loops, a lógica acima é ilustrativa para forçar o break após renderizar
        else:
            st.info("Aguardando sorteio dos confrontos.")

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
            
            # Carrega escalações
            q_esc = f"""
            SELECT liga_time_nome, atleta_posicao, atleta_apelido, pontos, is_capitao
            FROM `{client.project}.{DATASET_ID}.times_escalacoes`
            WHERE rodada = {rodada_sel}
            ORDER BY liga_time_nome, atleta_posicao
            """ # nosec B608
            df_esc = load_data(q_esc)
            
            # Filtro de Time
            times = df_esc['liga_time_nome'].unique()
            time_sel = st.multiselect("Filtrar Times:", times, default=times)
            
            df_final = df_esc[df_esc['liga_time_nome'].isin(time_sel)]
            
            # Pivot ou Exibição Simples
            st.dataframe(
                df_final.style.format({'pontos': '{:.2f}'}).applymap(
                    lambda x: 'font-weight: bold; color: blue' if x is True else '', subset=['is_capitao']
                ),
                use_container_width=True
            )
        else:
            st.warning("Sem dados de escalação.")
