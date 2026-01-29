import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES GERAIS ---
LIGA_SLUG = "sas-brasil-2026"

# Variáveis de Ambiente
BEARER_TOKEN = os.getenv('CARTOLA_BEARER_TOKEN')
GEMINI_KEY = os.getenv('GEMINI_API_KEY')
GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')

# Configurações BigQuery
DATASET_ID = "cartola_analytics"
TABELA_HISTORICO = f"{DATASET_ID}.historico"
TABELA_CORNETA = f"{DATASET_ID}.comentarios_ia"

# --- INFRAESTRUTURA ---
def get_bq_client():
    if not GCP_JSON:
        raise ValueError("Secret GCP_SERVICE_ACCOUNT não encontrada.")
    info_chave = json.loads(GCP_JSON)
    credentials = service_account.Credentials.from_service_account_info(info_chave)
    return bigquery.Client(credentials=credentials, project=info_chave['project_id'])

def garantir_dataset(client):
    dataset_ref = f"{client.project}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {DATASET_ID} garantido.")
    except Exception as e:
        print(f"Aviso dataset: {e}")

def salvar_bigquery(client, df, tabela_nome, schema=None):
    table_id = f"{client.project}.{tabela_nome}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema=schema)
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Sucesso: Dados salvos em {tabela_nome}")
    except Exception as e:
        print(f"❌ Erro BigQuery ({tabela_nome}): {e}")

# --- TRATAMENTO DE DADOS ---
def extrair_pontuacao(dado_pontos):
    if isinstance(dado_pontos, (int, float)):
        return float(dado_pontos)
    if isinstance(dado_pontos, dict):
        return float(dado_pontos.get('campeonato') or 0.0)
    return 0.0

# --- COLETA ---
def coletar_dados():
    url = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'x-glb-auth': 'oidc',
        'x-glb-app': 'cartola_web',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    }
    
    print(f"🔍 Buscando dados da liga: {LIGA_SLUG}...")
    res = requests.get(url, headers=headers)
    
    if res.status_code == 200:
        return res.json()
    else:
        print(f"❌ Erro API Cartola ({res.status_code}): {res.text}")
        return None

# --- IA (VIA REST API - SEM BIBLIOTECA) ---
def gerar_analise_ia(df_ranking):
    if not GEMINI_KEY:
        print("Chave Gemini não encontrada.")
        return "IA indisponível."
    
    # URL oficial da API REST do Gemini 2.0 Flash
    # Documentação: https://ai.google.dev/api/generate-content
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
    
    lider = df_ranking.iloc[0]['nome']
    lanterna = df_ranking.iloc[-1]['nome']
    pontos_lider = df_ranking.iloc[0]['pontos']
    
    prompt_texto = f"""
    Você é um narrador esportivo brasileiro sarcástico (estilo "Corneteiro").
    Resuma a situação atual da liga Cartola FC "{LIGA_SLUG}":
    - Líder: {lider} com {pontos_lider} pontos.
    - Lanterna: {lanterna}.
    
    Faça um comentário ácido e engraçado de no máximo 200 caracteres elogiando (ou dizendo que é sorte) o líder e zoando o lanterna.
    """

    payload = {
        "contents": [{
            "parts": [{"text": prompt_texto}]
        }]
    }
    
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, json=payload)
        
        if response.status_code == 200:
            resultado = response.json()
            # Navega no JSON de resposta para pegar o texto
            texto = resultado['candidates'][0]['content']['parts'][0]['text']
            return texto
        else:
            print(f"❌ Erro na API REST Gemini ({response.status_code}): {response.text}")
            return "O narrador está sem sinal (Erro na API)."
            
    except Exception as e:
        print(f"❌ Erro de conexão com Gemini: {e}")
        return "O narrador foi demitido (Erro técnico)."

# --- FLUXO PRINCIPAL ---
def main():
    if not BEARER_TOKEN:
        print("⛔ ERRO: Secret CARTOLA_BEARER_TOKEN não encontrada.")
        return

    dados = coletar_dados()
    if not dados: return

    times = dados.get('times', [])
    if not times:
        print("Nenhum time encontrado. Verifique Token/Slug.")
        return

    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    
    # 1. TRATAMENTO
    lista_limpa = []
    for time in times:
        lista_limpa.append({
            'nome': str(time['nome']),
            'nome_cartola': str(time['nome_cartola']),
            'pontos': extrair_pontuacao(time.get('pontos')),
            'patrimonio': float(time.get('patrimonio', 100)),
            'timestamp': ts_agora
        })

    df_historico = pd.DataFrame(lista_limpa)
    
    schema_historico = [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("nome_cartola", "STRING"),
        bigquery.SchemaField("pontos", "FLOAT"),
        bigquery.SchemaField("patrimonio", "FLOAT"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
    ]

    client = get_bq_client()
    garantir_dataset(client)
    
    # Salva Times
    salvar_bigquery(client, df_historico, TABELA_HISTORICO, schema_historico)
    
    # 2. GERAÇÃO DA IA
    ranking = df_historico.sort_values(by='pontos', ascending=False)
    
    print("🤖 Gerando comentário via REST API (Gemini 2.0)...")
    texto_ia = gerar_analise_ia(ranking)
    
    df_corneta = pd.DataFrame([{
        'texto': str(texto_ia), 
        'data': ts_agora
    }])
    
    schema_corneta = [
        bigquery.SchemaField("texto", "STRING"),
        bigquery.SchemaField("data", "TIMESTAMP"),
    ]
    
    # Salva Comentários
    salvar_bigquery(client, df_corneta, TABELA_CORNETA, schema_corneta)
    
    print("\n🚀 Automação concluída!")

if __name__ == "__main__":
    main()