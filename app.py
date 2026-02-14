import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import json
from google.oauth2 import service_account
from datetime import datetime
import altair as alt

# --- CONFIGURAÇÕES ---
st.set_page_config(page_title="Liga SAS Brasil 2026", page_icon="⚽", layout="wide")

def get_bq_client():
    creds, project_id = None, None
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        try:
            val = st.secrets["GCP_SERVICE_ACCOUNT"]
            info = json.loads(val) if isinstance(val, str) else dict(val)
            creds = service_account.Credentials.from_service_account_info(info)
            project_id = info['project_id']
        except: pass
    elif os.getenv('GCP_SERVICE_ACCOUNT'):
        try:
            info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
            creds = service_account.Credentials.from_service_account_info(info)
            project_id = info['project_id']
        except: pass
    elif os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    
    if creds and project_id: return bigquery.Client(credentials=creds, project=project_id)
    return None

client = get_bq_client()
DATASET_ID = "cartola_analytics"

# --- HELPERS ---
def get_dados_temporais():
    mes = datetime.now().month
    # Mapeamento de Meses
    mapa_mes = {
        1: ("pontos_jan_fev", "Jan/Fev"), 2: ("pontos_jan_fev", "Jan/Fev"),
        3: ("pontos_marco", "Março"), 4: ("pontos_abril", "Abril"),
        5: ("pontos_maio", "Maio"), 6: ("pontos_jun_jul", "Jun/Jul"),
        7: ("pontos_jun_jul", "Jun/Jul"), 8: ("pontos_agosto", "Agosto"),
        9: ("pontos_setembro", "Setembro"), 10: ("pontos_outubro", "Outubro"),
        11: ("pontos_nov_dez", "Nov/Dez"), 12: ("pontos_nov_dez", "Nov/Dez")
    }
    col_mes, nome_mes = mapa_mes.get(mes, ("pontos_jan_fev", "Início"))
    
    # Lógica do Turno (Automática baseada na rodada atual)
    # Assumindo rodada < 20 = 1º Turno
    col_turno = "pontos_turno_1"
    nome_turno = "1º Turno"
    # Se precisar dinâmico, teria que ler a rodada atual do banco, mas padrão é T1 agora.
    
    return col_mes, nome_mes, col_turno, nome_turno

@st.cache_data(ttl=300)
def load_data(query):
    if not client: return pd.DataFrame()
    try: return client.query(query).to_dataframe()
    except: return pd.DataFrame()

# --- INTERFACE ---
st.title("🏆 Liga SAS Brasil 2026")
tab1, tab2, tab3 = st.tabs(["⚽ Painel Principal", "🏆 Mata-Mata", "📋 Escalações"])

with tab1:
    if client:
        # --- 1. NARRADORES (Rodada acima, Geral abaixo) ---
        df_narrador = load_data(f"SELECT texto, tipo FROM `{client.project}.{DATASET_ID}.comentarios_ia` ORDER BY data DESC LIMIT 5") # nosec B608
        
        narr_rodada = df_narrador[df_narrador['tipo'] == 'RODADA'].head(1)
        narr_geral = df_narrador[df_narrador['tipo'] == 'GERAL'].head(1)

        if not narr_rodada.empty:
            st.info(f"🎙️ **Rodada:** {narr_rodada.iloc[0]['texto']}")
        if not narr_geral.empty:
            st.warning(f"🎙️ **Campeonato:** {narr_geral.iloc[0]['texto']}")

        st.divider()

        # --- CARGA DE DADOS ---
        df_view = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.view_consolidada_times`") # nosec B608
        
        q_last = f"""
        SELECT h.nome, h.pontos, h.rodada, h.tipo_dado 
        FROM `{client.project}.{DATASET_ID}.historico` h
        WHERE h.rodada = (SELECT MAX(rodada) FROM `{client.project}.{DATASET_ID}.historico`)
        ORDER BY h.pontos DESC
        """ # nosec B608
        df_rodada = load_data(q_last)

        if not df_view.empty and not df_rodada.empty:
            col_mes, nome_mes, col_turno, nome_turno = get_dados_temporais()
            
            # --- PREPARAÇÃO DOS DADOS (ORDENAÇÃO) ---
            top_geral = df_view.sort_values('total_geral', ascending=False)
            top_turno = df_view.sort_values(col_turno, ascending=False)
            top_mes = df_view.sort_values(col_mes, ascending=False) if col_mes in df_view.columns else pd.DataFrame()
            top_rodada = df_rodada # Já vem ordenada da query
            
            mitada = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
            zicada = df_view.sort_values('menor_pontuacao', ascending=True).iloc[0] # Exclui zeros via SQL
            rico = df_view.sort_values('patrimonio_atual', ascending=False).iloc[0]

            # --- LINHA 1: LIDERANÇAS (GERAL, TURNO, MÊS) ---
            c1, c2, c3 = st.columns(3)
            
            # GERAL
            lider_g = top_geral.iloc[0]
            vice_g = top_geral.iloc[1]
            c1.markdown("### 🥇 Geral")
            c1.metric("Líder", lider_g['nome'], f"{lider_g['total_geral']:.2f}")
            c1.metric("Vice", vice_g['nome'], f"{vice_g['total_geral']:.2f}", delta=f"{vice_g['total_geral'] - lider_g['total_geral']:.2f}")

            # TURNO
            lider_t = top_turno.iloc[0]
            vice_t = top_turno.iloc[1]
            c2.markdown(f"### 🥈 {nome_turno}")
            c2.metric("Líder", lider_t['nome'], f"{lider_t[col_turno]:.2f}")
            c2.metric("Vice", vice_t['nome'], f"{vice_t[col_turno]:.2f}", delta=f"{vice_t[col_turno] - lider_t[col_turno]:.2f}")

            # MÊS
            if not top_mes.empty:
                lider_m = top_mes.iloc[0]
                vice_m = top_mes.iloc[1]
                c3.markdown(f"### 📅 {nome_mes}")
                c3.metric("Líder", lider_m['nome'], f"{lider_m[col_mes]:.2f}")
                c3.metric("Vice", vice_m['nome'], f"{vice_m[col_mes]:.2f}", delta=f"{vice_m[col_mes] - lider_m[col_mes]:.2f}")
            
            st.divider()

            # --- LINHA 2: DESTAQUES (RODADA, MITADA, ZICADA, GRANA) ---
            k1, k2, k3, k4 = st.columns(4)
            
            # RODADA
            lider_r = top_rodada.iloc[0]
            vice_r = top_rodada.iloc[1]
            status_r = "(Ao Vivo)" if lider_r['tipo_dado'] == 'PARCIAL' else "(Fechada)"
            k1.metric(f"⚽ Mito R{lider_r['rodada']} {status_r}", lider_r['nome'], f"{lider_r['pontos']:.2f}")
            k1.caption(f"Vice: {vice_r['nome']} ({vice_r['pontos']:.2f})")

            # MITADA
            k2.metric("🚀 Maior Mitada", mitada['nome'], f"{mitada['maior_pontuacao']:.2f}")
            
            # ZICADA
            k3.metric("📉 Maior Zicada", zicada['nome'], f"{zicada['menor_pontuacao']:.2f}")
            
            # PATRIMÔNIO
            k4.metric("💰 O Mais Rico", rico['nome'], f"C$ {rico['patrimonio_atual']:.2f}")

            st.divider()

            # --- LINHA 3: GRÁFICOS (3 COLUNAS) ---
            g1, g2, g3 = st.columns(3)
            
            def plot_chart(df, x_col, title, color):
                return alt.Chart(df.head(5)).mark_bar().encode(
                    x=alt.X(x_col, title='Pontos'),
                    y=alt.Y('nome', sort='-x', title=None),
                    color=alt.value(color),
                    tooltip=['nome', x_col]
                ).properties(title=title)

            with g1: st.altair_chart(plot_chart(top_geral, 'total_geral', "Top 5 Geral", "#f9c74f"), use_container_width=True)
            with g2: st.altair_chart(plot_chart(top_turno, col_turno, f"Top 5 {nome_turno}", "#43aa8b"), use_container_width=True)
            with g3: 
                if not top_mes.empty: st.altair_chart(plot_chart(top_mes, col_mes, f"Top 5 {nome_mes}", "#577590"), use_container_width=True)

            # --- TABELA GERAL ---
            with st.expander("📊 Classificação Completa (Expandir)", expanded=True):
                # Seleção dinâmica de colunas
                cols_final = ['nome', 'nome_cartola', 'total_geral', col_turno, col_mes, 'media', 'maior_pontuacao', 'patrimonio_atual']
                # Filtra colunas que realmente existem no DF
                cols_validas = [c for c in cols_final if c in df_view.columns]
                
                # Renomeia para exibição bonita
                rename_map = {
                    'total_geral': 'Total', col_turno: nome_turno, col_mes: nome_mes,
                    'media': 'Média', 'maior_pontuacao': 'Maior Pts', 'patrimonio_atual': 'C$'
                }
                
                st.dataframe(
                    df_view[cols_validas].rename(columns=rename_map).style.format(precision=2),
                    use_container_width=True
                )
        else:
            st.warning("Dados sendo processados...")

with tab2:
    st.header("🏆 Mata-Mata")
    if client:
        df_copa = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.copa_mata_mata` ORDER BY data_coleta DESC") # nosec B608
        if not df_copa.empty:
            copa_sel = st.selectbox("Torneio:", df_copa['nome_copa'].unique())
            df_c = df_copa[df_copa['nome_copa'] == copa_sel]
            for fase in df_c['fase_copa'].unique():
                with st.expander(f"📍 {fase}", expanded=True):
                    for _, j in df_c[df_c['fase_copa'] == fase].iterrows():
                        c1, c2, c3 = st.columns([3, 2, 3])
                        win = str(j['vencedor'])
                        b_a = "**" if win in str(j['time_a_slug']) else ""
                        b_b = "**" if win in str(j['time_b_slug']) else ""
                        c1.markdown(f"<div style='text-align:right'>{b_a}{j['time_a_nome']}{b_a}</div>", unsafe_allow_html=True)
                        c2.markdown(f"<div style='text-align:center;background:#eee;border-radius:4px'>{j['time_a_pontos']:.2f} x {j['time_b_pontos']:.2f}</div>", unsafe_allow_html=True)
                        c3.markdown(f"<div style='text-align:left'>{b_b}{j['time_b_nome']}{b_b}</div>", unsafe_allow_html=True)
        else: st.info("Sem dados de Copa.")

with tab3:
    st.header("📋 Escalações")
    if client:
        df_r = load_data(f"SELECT DISTINCT rodada FROM `{client.project}.{DATASET_ID}.times_escalacoes` ORDER BY rodada DESC") # nosec B608
        if not df_r.empty:
            rodada = st.selectbox("Rodada:", df_r['rodada'].tolist())
            df_e = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.times_escalacoes` WHERE rodada = {rodada}") # nosec B608
            times = st.multiselect("Time:", df_e['liga_time_nome'].unique())
            if times:
                st.dataframe(df_e[df_e['liga_time_nome'].isin(times)], use_container_width=True)
        else: st.warning("Sem dados.")
