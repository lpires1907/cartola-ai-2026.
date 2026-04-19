import streamlit as st
import pandas as pd
from google.cloud import bigquery
import os
import sys
import json

# Adiciona a pasta src ao path para localizar os módulos movidos
sys.path.append(os.path.join(os.getcwd(), 'src'))

from google.oauth2 import service_account
from datetime import datetime
import altair as alt

# --- CONFIGURAÇÕES ---
st.set_page_config(page_title="Liga SAS Brasil 2026", page_icon="⚽", layout="wide")

def get_bq_client():
    def fix_credentials(info):
        if 'private_key' in info and isinstance(info['private_key'], str):
            pk = info['private_key'].replace('\\n', '\n').replace('\\\\n', '\n').strip()
            header = "-----BEGIN PRIVATE KEY-----"
            footer = "-----END PRIVATE KEY-----"
            if header in pk and footer in pk:
                try:
                    content = pk.split(header)[1].split(footer)[0].strip()
                    content = "".join(content.split())
                    import re
                    content_fixed = "\n".join(re.findall(r'.{1,64}', content))
                    pk = f"{header}\n{content_fixed}\n{footer}\n"
                except Exception: pass
            info['private_key'] = pk
        return info

    creds, project_id = None, None
    # 1. Secrets (Cloud)
    if "GCP_SERVICE_ACCOUNT" in st.secrets:
        try:
            val = st.secrets["GCP_SERVICE_ACCOUNT"]
            info = dict(val) if not isinstance(val, str) else json.loads(val)
            info = fix_credentials(info)
            creds = service_account.Credentials.from_service_account_info(info)
            project_id = info.get('project_id')
        except Exception as e:
            st.session_state["bq_auth_error"] = f"Erro ao processar Secret: {e}"
    elif "GCP_JSON_BASE64" in st.secrets:
        try:
            import base64
            b64_val = st.secrets["GCP_JSON_BASE64"]
            json_data = base64.b64decode(b64_val).decode('utf-8')
            info = fix_credentials(json.loads(json_data))
            creds = service_account.Credentials.from_service_account_info(info)
            project_id = info.get('project_id')
        except Exception as e:
            st.session_state["bq_auth_error"] = f"Erro ao decodificar Base64: {e}"
    # 2. Env Var (Local)
    elif os.getenv('GCP_SERVICE_ACCOUNT'):
        try:
            info = fix_credentials(json.loads(os.getenv('GCP_SERVICE_ACCOUNT')))
            creds = service_account.Credentials.from_service_account_info(info)
            project_id = info.get('project_id')
        except Exception as e:
            st.session_state["bq_auth_error"] = f"Erro ao processar .env: {e}"
    # 3. Arquivo
    elif os.path.exists("credentials.json"):
        try:
            return bigquery.Client.from_service_account_json("credentials.json")
        except Exception as e:
            st.session_state["bq_auth_error"] = f"Erro ao processar credentials.json: {e}"
    
    if creds and project_id: 
        try:
            return bigquery.Client(credentials=creds, project=project_id)
        except Exception as e:
            st.session_state["bq_auth_error"] = f"Erro ao criar cliente BigQuery: {e}"
    return None

client = get_bq_client()
DATASET_ID = "cartola_analytics"

# --- HELPERS ---
def get_dados_temporais(df_view):
    # Detecta a rodada real (máxima) para definir o mês ativo
    rodada_max = 0
    if not df_view.empty:
        if 'ultima_rodada' in df_view.columns:
            rodada_max = df_view['ultima_rodada'].max()
        elif 'rodadas_jogadas' in df_view.columns:
            rodada_max = df_view['rodadas_jogadas'].max()
    
    # Mapeamento de Rodada -> Mês (Baseado na NOVA REGRA)
    if rodada_max <= 4:
        col_mes, nome_mes = "pontos_jan_fev", "Jan/Fev"
    elif rodada_max <= 9:
        col_mes, nome_mes = "pontos_marco", "Março"
    elif rodada_max <= 13:
        col_mes, nome_mes = "pontos_abril", "Abril"
    elif rodada_max <= 17:
        col_mes, nome_mes = "pontos_maio", "Maio"
    elif rodada_max <= 21:
        col_mes, nome_mes = "pontos_junho", "Junho"
    elif rodada_max <= 25:
        col_mes, nome_mes = "pontos_julho", "Julho"
    elif rodada_max <= 29:
        col_mes, nome_mes = "pontos_agosto", "Agosto"
    elif rodada_max <= 33:
        col_mes, nome_mes = "pontos_setembro", "Setembro"
    elif rodada_max <= 36:
        col_mes, nome_mes = "pontos_outubro", "Outubro"
    else:
        col_mes, nome_mes = "pontos_nov_dez", "Nov/Dez"

    col_turno = "pontos_turno_1" if rodada_max <= 19 else "pontos_turno_2"
    nome_turno = "1º Turno" if rodada_max <= 19 else "2º Turno"
    
    return col_mes, nome_mes, col_turno, nome_turno

@st.cache_data(ttl=300)
def load_data(query):
    if not client:
        return pd.DataFrame()
    try:
        df = client.query(query).to_dataframe()
        cols_num = ['total_geral', 'pontos', 'media', 'maior_pontuacao', 'menor_pontuacao', 'patrimonio_atual']
        for col in df.columns:
            if col in cols_num or col.startswith('pontos_'):
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
        return df
    except Exception as e:
        # Surface error visibly so it appears in Streamlit logs and sidebar
        print(f"[BQ ERROR] {e}")
        st.session_state["last_bq_error"] = str(e)
        return pd.DataFrame()

# --- DIAGNÓSTICO (Sidebar) ---
with st.sidebar:
    st.markdown("### 🔧 Diagnóstico")
    
    # Detector de Secrets
    keys = list(st.secrets.keys())
    if "GCP_SERVICE_ACCOUNT" in keys:
        st.success("✅ Secret `GCP_SERVICE_ACCOUNT` detectado")
    else:
        st.error("❌ Secret `GCP_SERVICE_ACCOUNT` NÃO encontrado")
        st.write(f"Chaves disponíveis: `{keys}`")

    if client:
        st.success(f"✅ BQ conectado: `{client.project}`")
        # Contagem rápida de linhas para depuração
        try:
            count_h = client.query(f"SELECT COUNT(*) as c FROM `{client.project}.{DATASET_ID}.historico`").to_dataframe().iloc[0]['c']  # nosec
            count_e = client.query(f"SELECT COUNT(*) as c FROM `{client.project}.{DATASET_ID}.times_escalacoes`").to_dataframe().iloc[0]['c']  # nosec
            st.metric("Linhas em Historico", count_h)
            st.metric("Linhas em Escalacoes", count_e)
        except Exception as e:
            st.warning(f"⚠️ Erro ao contar linhas: {e}")
            if "pk_len" in st.session_state:
                with st.expander("🛠️ Inspetor de Chave (Debug)", expanded=True):
                    st.write(f"Tamanho: {st.session_state['pk_len']} caracteres")
                    st.code(f"Início: {st.session_state['pk_start']}")
                    st.code(f"Fim: {st.session_state['pk_end']}")
                    st.info("💡 Se o erro persistir, use a secret `GCP_JSON_BASE64` (mais robusta).")
    else:
        st.error("❌ BigQuery: sem conexão")
        auth_err = st.session_state.get("bq_auth_error")
        if auth_err:
            with st.expander("🔍 Detalhes do erro de Auth", expanded=True):
                st.write(auth_err)
    
    err = st.session_state.get("last_bq_error")
    if err:
        with st.expander("⚠️ Último erro de Query", expanded=True):
            st.code(err)

# --- INTERFACE ---
st.title("🏆 Liga SAS Brasil 2026")
tab1, tab2, tab3 = st.tabs(["⚽ Painel Principal", "🏆 Mata-Mata", "📋 Escalações"])

with tab1:
    if client:
        # 1. NARRADOR RODADA (Topo)
        df_narrador = load_data(f"SELECT texto, tipo FROM `{client.project}.{DATASET_ID}.comentarios_ia` ORDER BY data DESC LIMIT 5") # nosec B608
        if not df_narrador.empty and 'tipo' in df_narrador.columns:
            df_narrador['tipo'] = df_narrador['tipo'].str.upper()
            narr_rodada = df_narrador[df_narrador['tipo'] == 'RODADA'].head(1)
            narr_geral = df_narrador[df_narrador['tipo'] == 'GERAL'].head(1)
        else:
            narr_rodada, narr_geral = pd.DataFrame(), pd.DataFrame()

        if not narr_rodada.empty:
            st.info(f"🎙️ **Rodada:** {narr_rodada.iloc[0]['texto']}")

        st.divider()

        # 2. CARGA DE DADOS
        df_view = load_data(f"SELECT * FROM `{client.project}.{DATASET_ID}.view_consolidada_times`") # nosec B608
        q_last = f"""
        SELECT h.nome, h.pontos, h.rodada, h.tipo_dado 
        FROM `{client.project}.{DATASET_ID}.historico` h
        WHERE h.rodada = (SELECT MAX(rodada) FROM `{client.project}.{DATASET_ID}.historico`)
        ORDER BY h.pontos DESC
        """ # nosec B608
        df_rodada = load_data(q_last)

        if not df_view.empty and not df_rodada.empty:
            col_mes, nome_mes, col_turno, nome_turno = get_dados_temporais(df_view)
            
            # ORDENAÇÕES
            top_geral = df_view.sort_values('total_geral', ascending=False)
            top_turno = df_view.sort_values(col_turno, ascending=False)
            top_mes = df_view.sort_values(col_mes, ascending=False) if col_mes in df_view.columns else pd.DataFrame()
            
            # KPIs EXTRAS
            mitada = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
            df_zica = df_view[df_view['menor_pontuacao'] > 0.1].sort_values('menor_pontuacao', ascending=True)
            zicada = df_zica.iloc[0] if not df_zica.empty else df_view.iloc[0]
            # patrimônio column name can vary between schema versions
            possiveis_cols = ['patrimonio_atual', 'patrimonio_medio', 'patrimonio']
            col_patrimonio = next((c for c in possiveis_cols if c in df_view.columns), None)
            rico = df_view.sort_values(col_patrimonio, ascending=False).iloc[0] if col_patrimonio else None

            # --- LINHA 1: LIDERANÇAS ---
            c1, c2, c3 = st.columns(3)
            
            # GERAL
            if len(top_geral) > 0:
                lg = top_geral.iloc[0]
                c1.markdown("### 🥇 Geral")
                c1.metric("Líder", lg['nome'], f"{lg['total_geral']:.2f}")
                if len(top_geral) > 1:
                    vg = top_geral.iloc[1]
                    diff = vg['total_geral'] - lg['total_geral']
                    # CORREÇÃO: Passando argumentos explicitamente
                    c1.metric(f"🥈 Vice: {vg['nome']}", f"{vg['total_geral']:.2f}", delta=f"{diff:.2f}")

            # TURNO
            if len(top_turno) > 0:
                lt = top_turno.iloc[0]
                c2.markdown(f"### 🥈 {nome_turno}")
                c2.metric("Líder", lt['nome'], f"{lt[col_turno]:.2f}")
                if len(top_turno) > 1:
                    vt = top_turno.iloc[1]
                    diff_t = vt[col_turno] - lt[col_turno]
                    c2.metric(f"Vice: {vt['nome']}", f"{vt[col_turno]:.2f}", delta=f"{diff_t:.2f}")

            # MÊS
            if not top_mes.empty and len(top_mes) > 0:
                lm = top_mes.iloc[0]
                c3.markdown(f"### 📅 {nome_mes}")
                c3.metric("Líder", lm['nome'], f"{lm[col_mes]:.2f}")
                if len(top_mes) > 1:
                    vm = top_mes.iloc[1]
                    diff_m = vm[col_mes] - lm[col_mes]
                    c3.metric(f"Vice: {vm['nome']}", f"{vm[col_mes]:.2f}", delta=f"{diff_m:.2f}")
            else:
                c3.markdown(f"### 📅 {nome_mes}")
                c3.info("Início de mês.")
            
            st.divider()

            # --- LINHA 2: MITADAS & GRANA ---
            k1, k2, k3, k4 = st.columns(4)
            
            # Rodada
            lr = df_rodada.iloc[0]
            status_r = "(Ao Vivo)" if lr['tipo_dado'] == 'PARCIAL' else "(Fechada)"
            k1.metric(f"⚽ Mito R{lr['rodada']} {status_r}", lr['nome'], f"{lr['pontos']:.2f}")
            if len(df_rodada) > 1:
                vr = df_rodada.iloc[1]
                k1.caption(f"Vice: {vr['nome']} ({vr['pontos']:.2f})")

            k2.metric("🚀 Maior Mitada", mitada['nome'], f"{mitada['maior_pontuacao']:.2f}")
            k3.metric("📉 Maior Zicada", zicada['nome'], f"{zicada['menor_pontuacao']:.2f}")
            if rico is not None and col_patrimonio:
                k4.metric("💰 O Mais Rico", rico['nome'], f"C$ {rico[col_patrimonio]:.2f}")
            else:
                k4.info("Patrimônio indisponível.")

            # NARRADOR GERAL (Embaixo)
            if not narr_geral.empty:
                st.write("")
                st.warning(f"🎙️ **Resenha do Campeonato:** {narr_geral.iloc[0]['texto']}")

            st.divider()

            # --- GRÁFICOS ---
            g1, g2, g3 = st.columns(3)
            def plot_chart(df, x_col, title, color):
                return alt.Chart(df.head(5)).mark_bar().encode(
                    x=alt.X(x_col, title='Pontos'),
                    y=alt.Y('nome', sort='-x', title=None),
                    color=alt.value(color), tooltip=['nome', x_col]
                ).properties(title=title)

            with g1: st.altair_chart(plot_chart(top_geral, 'total_geral', "Top 5 Geral", "#f9c74f"), use_container_width=True)
            with g2: st.altair_chart(plot_chart(top_turno, col_turno, f"Top 5 {nome_turno}", "#43aa8b"), use_container_width=True)
            with g3: 
                if not top_mes.empty: st.altair_chart(plot_chart(top_mes, col_mes, f"Top 5 {nome_mes}", "#577590"), use_container_width=True)

            # --- TABELA GERAL ---
            with st.expander("📊 Tabela Completa (Expandir)", expanded=True):
                cols_r = ['nome', 'nome_cartola', 'total_geral', col_turno, col_mes, 'media', 'maior_pontuacao']
                if col_patrimonio:
                    cols_r.append(col_patrimonio)
                # Filtra colunas existentes e úteis
                cols_r = ['nome', 'nome_cartola', 'total_geral', col_turno, col_mes, 'media', 'maior_pontuacao', 'rodadas_jogadas', col_patrimonio]
                cols_v = [c for c in cols_r if c and c in df_view.columns]
                ren = {'total_geral': 'Total', col_turno: nome_turno, col_mes: nome_mes, 'media': 'Média', 'maior_pontuacao': 'Recorde'}
                # Tratamento especial para nomes de meses novos na View
                for m_col in ['pontos_junho', 'pontos_julho']:
                    if m_col in df_view.columns and m_col not in ren:
                        ren[m_col] = m_col.replace('pontos_', '').capitalize()
                if col_patrimonio:
                    ren[col_patrimonio] = 'C$'
                st.dataframe(df_view[cols_v].rename(columns=ren).style.format(precision=2), use_container_width=True)
        else:
            st.warning("Aguardando dados... Rode o coletor.")

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
                        w = str(j['vencedor'])
                        ba = "**" if w in str(j['time_a_slug']) else ""
                        bb = "**" if w in str(j['time_b_slug']) else ""
                        c1.markdown(f"<div style='text-align:right'>{ba}{j['time_a_nome']}{ba}</div>", unsafe_allow_html=True)
                        c2.markdown(f"<div style='text-align:center;background:#eee;border-radius:4px'>{j['time_a_pontos']:.2f} x {j['time_b_pontos']:.2f}</div>", unsafe_allow_html=True)
                        c3.markdown(f"<div style='text-align:left'>{bb}{j['time_b_nome']}{bb}</div>", unsafe_allow_html=True)
        else: st.info("Sem dados de Copa.")

with tab3:
    st.header("📋 Escalações")
    if client:
        df_r = load_data(f"SELECT DISTINCT rodada FROM `{client.project}.{DATASET_ID}.times_escalacoes` ORDER BY rodada DESC") # nosec B608
        if not df_r.empty:
            rodada = st.selectbox("Rodada:", df_r['rodada'].tolist())
            q_esc = f"""
            SELECT liga_time_nome, atleta_posicao, atleta_apelido, pontos, is_capitao
            FROM `{client.project}.{DATASET_ID}.times_escalacoes` WHERE rodada = {rodada}
            """ # nosec B608
            df_e = load_data(q_esc)
            times = st.multiselect("Time:", df_e['liga_time_nome'].unique() if not df_e.empty else [])
            if times:
                st.dataframe(df_e[df_e['liga_time_nome'].isin(times)].style.format({'pontos': '{:.2f}'}).map(
                    lambda x: 'color:blue;font-weight:bold' if x is True else '', subset=['is_capitao']
                ), use_container_width=True)
        else: st.warning("Sem dados.")
