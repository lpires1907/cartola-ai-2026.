import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES ---
GEMINI_KEY = os.getenv('GEMINI_API_KEY')
GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_CORNETA = f"{DATASET_ID}.comentarios_ia"

def get_bq_client():
    info = json.loads(GCP_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

def ja_comentou_rodada_fechada(client, rodada):
    """Verifica se já existe comentário para esta rodada oficial"""
    try:
        query = f"""
            SELECT COUNT(*) as qtd FROM `{client.project}.{TAB_CORNETA}` 
            WHERE rodada = {rodada} 
            AND (texto NOT LIKE '%pré-temporada%' AND texto NOT LIKE '%AO VIVO%')
        """
        res = list(client.query(query).result())
        return res[0].qtd > 0
    except: return False

def gerar_analise_gemini(df_ranking, rodada, status_rodada):
    if not GEMINI_KEY: return "IA sem contrato."

    qtd = len(df_ranking)
    lider = df_ranking.iloc[0]
    lanterna = df_ranking.iloc[-1]
    
    vice_lider = df_ranking.iloc[1] if qtd >= 2 else None
    vice_lanterna = df_ranking.iloc[-2] if qtd >= 3 else None
    
    txt_status = "AO VIVO (Parcial)" if status_rodada == 'PARCIAL' else "FINALIZADA"

    prompt = f"""
    Você é um comentarista de futebol e Fantasy Game (Cartola FC) sarcástico e ácido.
    Analise a RODADA {rodada} ({txt_status}) da liga.

    DADOS:
    1. 🥇 LÍDER: {lider['nome']} fez {lider['pontos']:.1f} pts (Patrimônio: C$ {lider.get('patrimonio', 100):.1f}).
    {f"2. 🥈 VICE-LÍDER: {vice_lider['nome']} fez {vice_lider['pontos']:.1f} pts (Está na cola!)." if vice_lider is not None else ""}
    
    ... (meio da tabela) ...

    {f"3. 🥉 VICE-LANTERNA: {vice_lanterna['nome']} fez {vice_lanterna['pontos']:.1f} pts (Por pouco!)." if vice_lanterna is not None else ""}
    4. 🐌 LANTERNA: {lanterna['nome']} fez {lanterna['pontos']:.1f} pts (Patrimônio: C$ {lanterna.get('patrimonio', 100):.1f}).

    MISSÃO:
    Escreva um parágrafo curto (max 400 caracteres) zoando o lanterna e o vice-lanterna, e alertando o líder que o vice está chegando (ou elogiando a sorte do líder).
    Use emojis. Se for rodada AO VIVO, diga que "ainda tem jogo". Se for FINALIZADA, decrete o resultado.
    """

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
    
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    headers = {'Content-Type': 'application/json'}
    
    try:
        res = requests.post(url, headers=headers, json=payload)
        if res.status_code == 200:
            return res.json()['candidates'][0]['content']['parts'][0]['text']
        else:
            return f"Erro IA: {res.status_code}"
    except Exception as e:
        return f"Erro Narrador: {e}"

def main():
    if not GCP_JSON: print("Erro: Sem credenciais GCP"); return
    client = get_bq_client()

    # 1. Pega os dados mais recentes do banco
    query = f"""
        SELECT * FROM `{client.project}.{TAB_HISTORICO}`
        WHERE timestamp = (SELECT MAX(timestamp) FROM `{client.project}.{TAB_HISTORICO}`)
        ORDER BY pontos DESC
    """
    df_ranking = client.query(query).to_dataframe()

    if df_ranking.empty:
        print("📭 Nenhum dado no histórico para comentar.")
        return

    # --- CORREÇÃO DO ERRO AQUI ---
    rodada_banco = int(df_ranking.iloc[0]['rodada'])
    
    # Verifica se a coluna existe. Se não existir (dados antigos), assume OFICIAL.
    if 'tipo_dado' in df_ranking.columns:
        tipo_dado = df_ranking.iloc[0]['tipo_dado']
    else:
        tipo_dado = 'OFICIAL' 
    # -----------------------------

    print(f"🎤 Preparando narração para Rodada {rodada_banco} ({tipo_dado})...")

    # 2. Check de redundância
    if tipo_dado == 'OFICIAL' and ja_comentou_rodada_fechada(client, rodada_banco):
        print("🤐 Rodada oficial já comentada anteriormente. Narrador em silêncio.")
        return

    # 3. Gera o comentário
    texto = gerar_analise_gemini(df_ranking, rodada_banco, tipo_dado)
    print(f"🗣️ Comentário gerado: {texto}")

    # 4. Salva
    df_save = pd.DataFrame([{
        'texto': texto,
        'rodada': rodada_banco,
        'data': datetime.now(pytz.timezone('America/Sao_Paulo'))
    }])
    
    # Salva no BQ
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=[
        bigquery.SchemaField("texto", "STRING"), bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("data", "TIMESTAMP")
    ])
    client.load_table_from_dataframe(df_save, f"{client.project}.{TAB_CORNETA}", job_config=job_config).result()
    print("💾 Comentário salvo com sucesso!")

if __name__ == "__main__":
    main()