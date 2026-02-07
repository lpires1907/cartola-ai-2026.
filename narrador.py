import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz
from google import genai 

# Importação segura do dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# --- CONFIGURAÇÕES ---
GEMINI_KEY = os.getenv('GEMINI_API_KEY')
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_CORNETA = f"{DATASET_ID}.comentarios_ia"
VIEW_CONSOLIDADA = f"{DATASET_ID}.view_consolidada_times"

MODEL_VERSION = "gemini-2.5-flash" 

def get_bq_client():
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    
    if os.getenv('GCP_SERVICE_ACCOUNT'):
        try:
            if isinstance(os.getenv('GCP_SERVICE_ACCOUNT'), str):
                info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
            else:
                info = os.getenv('GCP_SERVICE_ACCOUNT')
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=creds, project=info['project_id'])
        except: pass
            
    return bigquery.Client()

def ja_comentou(client, rodada, tipo):
    try:
        query = f"""
            SELECT COUNT(*) as qtd FROM `{client.project}.{TAB_CORNETA}` 
            WHERE rodada = {rodada} AND tipo = '{tipo}'
        """ # nosec
        res = list(client.query(query).result())
        return res[0].qtd > 0
    except: return False

def chamar_gemini(prompt):
    if not GEMINI_KEY: 
        print("⚠️ GEMINI_API_KEY não encontrada.")
        return None
    try:
        client = genai.Client(api_key=GEMINI_KEY)
        response = client.models.generate_content(
            model=MODEL_VERSION,
            contents=prompt
        )
        return response.text
    except Exception as e:
        print(f"❌ Erro no SDK google-genai: {e}")
        return None

# --- AUXILIAR: DESCOBRIR COLUNAS DO MÊS ---
def get_coluna_mes(rodada):
    if rodada <= 8: return "pontos_jan_fev", "Jan/Fev"
    if rodada <= 12: return "pontos_marco", "Março"
    if rodada <= 16: return "pontos_abril", "Abril"
    if rodada <= 20: return "pontos_maio", "Maio"
    if rodada <= 24: return "pontos_jun_jul", "Jun/Jul"
    if rodada <= 29: return "pontos_agosto", "Agosto"
    if rodada <= 33: return "pontos_setembro", "Setembro"
    if rodada <= 36: return "pontos_outubro", "Outubro"
    return "pontos_nov_dez", "Nov/Dez"

# 1. Narrador da RODADA (Micro)
def gerar_analise_rodada(df_ranking, rodada, status_rodada):
    lider = df_ranking.iloc[0]
    vice = df_ranking.iloc[1] if len(df_ranking) > 1 else lider
    
    # === CORREÇÃO: FILTRA QUEM ZEROU (Quem não jogou) ===
    df_jogaram = df_ranking[df_ranking['pontos'] > 0]
    
    if df_jogaram.empty:
        # Fallback extremo se ninguém pontuou
        lanterna = df_ranking.iloc[-1]
        vice_lanterna = df_ranking.iloc[-2] if len(df_ranking) > 1 else lanterna
    else:
        lanterna = df_jogaram.iloc[-1]
        vice_lanterna = df_jogaram.iloc[-2] if len(df_jogaram) > 1 else lanterna
    # ====================================================

    txt_status = "AO VIVO" if status_rodada == 'PARCIAL' else "FINALIZADA"
    
    prompt = f"""
    Atue como um narrador de futebol brasileiro sarcástico e divertido.
    Resumo da Rodada {rodada} ({txt_status}).
    
    DADOS DA RODADA:
    1. O Mito (1º lugar): {lider['nome']} com {lider['pontos']} pts.
    2. O "Quase" (2º lugar): {vice['nome']} com {vice['pontos']} pts.
    
    OS BAGRES DA RODADA (Quem jogou e foi mal):
    1. A Zicada Suprema (Último > 0): {lanterna['nome']} com {lanterna['pontos']} pts.
    2. O Vice-Zica (Penúltimo > 0): {vice_lanterna['nome']} com {vice_lanterna['pontos']} pts.
    
    INSTRUÇÃO:
    Faça um comentário curto (max 280 chars) exaltando o mito e humilhando a zicada.
    """
    return chamar_gemini(prompt)

# 2. Narrador GERAL (Macro)
def gerar_analise_geral(df_view, rodada_atual):
    # 1. Líder Geral
    lider_geral = df_view.sort_values('total_geral', ascending=False).iloc[0]
    vice_geral = df_view.sort_values('total_geral', ascending=False).iloc[1]
    
    # 2. Líder do Turno
    col_turno = 'pontos_turno_2' if rodada_atual >= 19 else 'pontos_turno_1'
    nome_turno = "2º Turno" if rodada_atual >= 19 else "1º Turno"
    lider_turno = df_view.sort_values(col_turno, ascending=False).iloc[0]
    
    # 3. Líder do Mês
    col_mes, nome_mes = get_coluna_mes(rodada_atual)
    if col_mes in df_view.columns:
        lider_mes = df_view.sort_values(col_mes, ascending=False).iloc[0]
        dados_mes = f"- Destaque de {nome_mes}: {lider_mes['nome']} ({lider_mes[col_mes]:.1f} pts)."
    else:
        dados_mes = ""

    distancia = lider_geral['total_geral'] - vice_geral['total_geral']
    
    prompt = f"""
    Analista esportivo ácido e detalhista. Resumo do campeonato até Rodada {rodada_atual}.
    
    OS DONOS DO JOGO:
    - Líder Geral (Campeonato): {lider_geral['nome']} (Vantagem de {distancia:.1f} pts sobre {vice_geral['nome']}).
    - Líder do {nome_turno}: {lider_turno['nome']} ({lider_turno[col_turno]:.1f} pts).
    {dados_mes}
    
    INSTRUÇÃO:
    Escreva um parágrafo (max 400 chars).
    Explique quem manda no campeonato. Se for o mesmo time liderando tudo, diga que é uma dinastia.
    Se forem times diferentes, diga que a disputa está aberta.
    """
    return chamar_gemini(prompt)

def salvar_comentario(client, texto, rodada, tipo, ts):
    df = pd.DataFrame([{'texto': texto, 'rodada': rodada, 'tipo': tipo, 'data': ts}])
    schema = [bigquery.SchemaField("texto", "STRING"), bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("tipo", "STRING"), bigquery.SchemaField("data", "TIMESTAMP")]
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION], schema=schema)
    try: client.load_table_from_dataframe(df, f"{client.project}.{TAB_CORNETA}", job_config=job_config).result()
    except: pass

def gerar_narracao_rodada():
    client = get_bq_client()
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    print("🎤 Narrador entrando em campo...")

    try:
        q_meta = f"SELECT rodada, tipo_dado FROM `{client.project}.{TAB_HISTORICO}` ORDER BY timestamp DESC LIMIT 1" # nosec
        df_meta = client.query(q_meta).to_dataframe()
        if df_meta.empty: return

        rodada = int(df_meta['rodada'].iloc[0])
        status = df_meta['tipo_dado'].iloc[0]

        # 1. Rodada
        if not ja_comentou(client, rodada,  'RODADA'):
            df_round = client.query(f"SELECT * FROM `{client.project}.{TAB_HISTORICO}` WHERE rodada = {rodada} ORDER BY pontos DESC").to_dataframe() # nosec
            txt = gerar_analise_rodada(df_round, rodada, status)
            if txt:
                print(f"💬 Rodada: {txt}")
                salvar_comentario(client, txt, rodada, 'RODADA', ts_agora)

        # 2. Geral (Inclui Turno e Mês agora)
        if status == 'OFICIAL' and not ja_comentou(client, rodada, 'GERAL'):
            df_view = client.query(f"SELECT * FROM `{client.project}.{VIEW_CONSOLIDADA}`").to_dataframe() # nosec
            txt_geral = gerar_analise_geral(df_view, rodada)
            if txt_geral:
                print(f"💬 Geral: {txt_geral}")
                salvar_comentario(client, txt_geral, rodada, 'GERAL', ts_agora)

    except Exception as e:
        print(f"❌ Erro Narrador: {e}")

if __name__ == "__main__":
    gerar_narracao_rodada()
