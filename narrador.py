import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz
from google import genai 

# --- CONFIGURAÇÕES ---
GEMINI_KEY = os.getenv('GEMINI_API_KEY')
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_CORNETA = f"{DATASET_ID}.comentarios_ia"
VIEW_CONSOLIDADA = f"{DATASET_ID}.view_consolidada_times"

# Modelo
MODEL_VERSION = "gemini-2.5-flash" 

def get_bq_client():
    info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

# --- CONTROLE DE REDUNDÂNCIA ---
def ja_comentou(client, rodada, tipo):
    try:
        query = f"""
            SELECT COUNT(*) as qtd FROM `{client.project}.{TAB_CORNETA}` 
            WHERE rodada = {rodada} AND tipo = '{tipo}'
        """
        res = list(client.query(query).result())
        return res[0].qtd > 0
    except: return False

# --- GERAÇÃO DE TEXTO (SDK google-genai) ---
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

def gerar_analise_rodada(df_ranking, rodada, status_rodada):
    # Topo da tabela
    lider = df_ranking.iloc[0]
    vice_lider = df_ranking.iloc[1] if len(df_ranking) > 1 else None
    
    # Fundo da tabela
    lanterna = df_ranking.iloc[-1]
    vice_lanterna = df_ranking.iloc[-2] if len(df_ranking) > 1 else None
    
    txt_status = "AO VIVO" if status_rodada == 'PARCIAL' else "FINALIZADA"
    
    prompt = f"""
    Atue como um narrador de futebol brasileiro sarcástico e ácido. Rodada {rodada} ({txt_status}).
    
    O TOPO:
    1. Líder: {lider['nome']} ({lider['pontos']} pts) - Omitiu a concorrência.
    2. Sombra: {vice_lider['nome']} ({vice_lider['pontos']} pts) - Respirando no cangote.
    
    O FUNDO DO POÇO:
    1. Lanterna: {lanterna['nome']} ({lanterna['pontos']} pts) - O pior do dia.
    2. Vice-Lanterna: {vice_lanterna['nome']} ({vice_lanterna['pontos']} pts) - Escapou da vergonha maior por pouco.
    
    MISSÃO:
    Faça um comentário curto (max 280 chars) zoando o lanterna e o vice-lanterna, e alertando o líder sobre o vice. Use gírias de estádio.
    """
    return chamar_gemini(prompt)

def gerar_analise_geral(df_view, rodada_atual):
    lider_geral = df_view.iloc[0]
    vice_geral = df_view.iloc[1] if len(df_view) > 1 else None
    
    maior_media = df_view.sort_values('media', ascending=False).iloc[0]
    maior_pico = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
    
    distancia_lider = (lider_geral['total_geral'] - vice_geral['total_geral']) if vice_geral is not None else 0
    
    prompt = f"""
    Analista de dados esportivos (com humor ácido). Campeonato Geral até rodada {rodada_atual}.
    
    DISPUTA PELO TÍTULO:
    - Líder: {lider_geral['nome']} (Total: {lider_geral['total_geral']:.1f}).
    - Vice: {vice_geral['nome']} (Total: {vice_geral['total_geral']:.1f}).
    - Diferença: {distancia_lider:.1f} pontos.
    
    ESTATÍSTICAS:
    - Melhor Média: {maior_media['nome']} ({maior_media['media']:.1f}).
    - Maior Mitada Única: {maior_pico['nome']} ({maior_pico['maior_pontuacao']:.1f}).
    
    Resuma em um parágrafo. O campeonato está aberto com essa diferença de {distancia_lider:.1f} pontos ou o líder já encomendou a faixa?
    """
    return chamar_gemini(prompt)

# --- PERSISTÊNCIA ---
def salvar_comentario(client, texto, rodada, tipo, ts):
    df = pd.DataFrame([{'texto': texto, 'rodada': rodada, 'tipo': tipo, 'data': ts}])
    schema = [
        bigquery.SchemaField("texto", "STRING"),
        bigquery.SchemaField("rodada", "INTEGER"),
        bigquery.SchemaField("tipo", "STRING"),
        bigquery.SchemaField("data", "TIMESTAMP")
    ]
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=schema
    )
    client.load_table_from_dataframe(df, f"{client.project}.{TAB_CORNETA}", job_config=job_config).result()

# --- EXPORTÁVEL ---
def rodar_narracao():
    client = get_bq_client()
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))

    # Pega metadados
    query_meta = f"SELECT rodada, tipo_dado FROM `{client.project}.{TAB_HISTORICO}` ORDER BY timestamp DESC LIMIT 1"
    df_meta = client.query(query_meta).to_dataframe()
    
    if df_meta.empty: return
    rodada_atual = int(df_meta['rodada'].iloc[0])
    status_dados = df_meta['tipo_dado'].iloc[0]

    print(f"🎤 Narrador (v2.5) analisando Rodada {rodada_atual}...")

    # Micro Análise (Rodada)
    if not ja_comentou(client, rodada_atual, 'RODADA'):
        df_round = client.query(f"SELECT * FROM `{client.project}.{TAB_HISTORICO}` WHERE rodada = {rodada_atual} ORDER BY pontos DESC").to_dataframe()
        texto = gerar_analise_rodada(df_round, rodada_atual, status_dados)
        if texto:
            salvar_comentario(client, texto, rodada_atual, 'RODADA', ts_agora)
            print("💾 Corneta da rodada salva!")

    # Macro Análise (Geral)
    if status_dados == 'OFICIAL' and not ja_comentou(client, rodada_atual, 'GERAL'):
        try:
            df_view = client.query(f"SELECT * FROM `{client.project}.{VIEW_CONSOLIDADA}`").to_dataframe()
            texto_geral = gerar_analise_geral(df_view, rodada_atual)
            if texto_geral:
                salvar_comentario(client, texto_geral, rodada_atual, 'GERAL', ts_agora)
                print("💾 Análise geral salva!")
        except Exception as e:
            print(f"⚠️ Erro ao ler view consolidada: {e}")

if __name__ == "__main__":
    rodar_narracao()