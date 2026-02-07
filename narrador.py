import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz
from google import genai 
from dotenv import load_dotenv

# Carrega ambiente local se necessário
load_dotenv()

# --- CONFIGURAÇÕES ---
GEMINI_KEY = os.getenv('GEMINI_API_KEY')
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_CORNETA = f"{DATASET_ID}.comentarios_ia"
VIEW_CONSOLIDADA = f"{DATASET_ID}.view_consolidada_times"

MODEL_VERSION = "gemini-2.5-flash" 

def get_bq_client():
    # Tenta pegar credenciais do ambiente ou arquivo local
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

# --- CONTROLE DE REDUNDÂNCIA ---
def ja_comentou(client, rodada, tipo):
    try:
        query = f"""
            SELECT COUNT(*) as qtd FROM `{client.project}.{TAB_CORNETA}` 
            WHERE rodada = {rodada} AND tipo = '{tipo}'
        """ # nosec
        res = list(client.query(query).result())
        return res[0].qtd > 0
    except: return False

# --- GERAÇÃO DE TEXTO ---
def chamar_gemini(prompt):
    if not GEMINI_KEY: 
        print("⚠️ GEMINI_API_KEY não encontrada. Pulando IA.")
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
    lider = df_ranking.iloc[0]
    vice_lider = df_ranking.iloc[1] if len(df_ranking) > 1 else None
    lanterna = df_ranking.iloc[-1]
    vice_lanterna = df_ranking.iloc[-2] if len(df_ranking) > 1 else None
    
    txt_status = "AO VIVO" if status_rodada == 'PARCIAL' else "FINALIZADA"
    
    prompt = f"""
    Atue como um narrador de futebol brasileiro sarcástico e ácido. Rodada {rodada} ({txt_status}).
    
    O TOPO:
    1. Líder: {lider['nome']} ({lider['pontos']} pts).
    2. Sombra: {vice_lider['nome']} ({vice_lider['pontos']} pts).
    
    O FUNDO:
    1. Lanterna: {lanterna['nome']} ({lanterna['pontos']} pts).
    2. Vice-Lanterna: {vice_lanterna['nome']} ({vice_lanterna['pontos']} pts).
    
    Faça um comentário curto (max 280 chars) zoando o lanterna e alertando o líder.
    """
    return chamar_gemini(prompt)

def gerar_analise_geral(df_view, rodada_atual):
    lider_geral = df_view.iloc[0]
    vice_geral = df_view.iloc[1] if len(df_view) > 1 else None
    
    # Previne erro se estiver vazio
    if lider_geral is None: return None

    maior_media = df_view.sort_values('media', ascending=False).iloc[0]
    maior_pico = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
    
    distancia_lider = (lider_geral['total_geral'] - vice_geral['total_geral']) if vice_geral is not None else 0
    
    prompt = f"""
    Analista esportivo sarcástico. Geral até rodada {rodada_atual}.
    - Líder: {lider_geral['nome']} (Total: {lider_geral['total_geral']:.1f}).
    - Vice: {vice_geral['nome']} (Total: {vice_geral['total_geral']:.1f}).
    - Diferença: {distancia_lider:.1f} pts.
    - Maior Mitada: {maior_pico['nome']} ({maior_pico['maior_pontuacao']:.1f}).
    
    Resuma em um parágrafo. O campeonato está aberto ou o líder disparou?
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
    try:
        client.load_table_from_dataframe(df, f"{client.project}.{TAB_CORNETA}", job_config=job_config).result()
        print(f"💾 Comentário ({tipo}) salvo no BigQuery!")
    except Exception as e:
        print(f"❌ Erro ao salvar comentário: {e}")

# --- FUNÇÃO PRINCIPAL (RENOMEADA PARA COMPATIBILIDADE) ---
def gerar_narracao_rodada():
    """
    Função principal chamada pelo main.py
    """
    client = get_bq_client()
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))

    print("🎤 Narrador (IA Sarcástica) entrando em campo...")

    try:
        # Busca metadados da última rodada
        query_meta = f"SELECT rodada, tipo_dado FROM `{client.project}.{TAB_HISTORICO}` ORDER BY timestamp DESC LIMIT 1" # nosec
        df_meta = client.query(query_meta).to_dataframe()
        
        if df_meta.empty: 
            print("⚠️ Sem dados históricos para narrar.")
            return

        rodada_atual = int(df_meta['rodada'].iloc[0])
        status_dados = df_meta['tipo_dado'].iloc[0]

        print(f"📢 Analisando Rodada {rodada_atual} ({status_dados})...")

        # 1. Micro Análise (Rodada)
        if not ja_comentou(client, rodada_atual, 'RODADA'):
            df_round = client.query(f"SELECT * FROM `{client.project}.{TAB_HISTORICO}` WHERE rodada = {rodada_atual} ORDER BY pontos DESC").to_dataframe() # nosec
            texto = gerar_analise_rodada(df_round, rodada_atual, status_dados)
            if texto:
                print(f"💬 Corneta da Rodada: {texto}")
                salvar_comentario(client, texto, rodada_atual, 'RODADA', ts_agora)
        else:
            print("💤 Corneta da rodada já foi feita.")

        # 2. Macro Análise (Geral - Apenas se for oficial)
        if status_dados == 'OFICIAL' and not ja_comentou(client, rodada_atual, 'GERAL'):
            try:
                # Importante: Usa a View Consolidada que o processamento.py acabou de recriar
                df_view = client.query(f"SELECT * FROM `{client.project}.{VIEW_CONSOLIDADA}` ORDER BY total_geral DESC").to_dataframe() # nosec
                texto_geral = gerar_analise_geral(df_view, rodada_atual)
                if texto_geral:
                    print(f"💬 Análise Geral: {texto_geral}")
                    salvar_comentario(client, texto_geral, rodada_atual, 'GERAL', ts_agora)
            except Exception as e:
                print(f"⚠️ Erro na análise geral: {e}")
        else:
            if status_dados != 'OFICIAL':
                print("⏳ Aguardando fechamento oficial para análise geral.")
            else:
                print("💤 Análise geral já foi feita.")

    except Exception as e:
        print(f"❌ O Narrador engasgou: {e}")

if __name__ == "__main__":
    gerar_narracao_rodada()