import os
import json
import requests
import pandas as pd
import google.generativeai as genai
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES DE AMBIENTE ---
# Agora usamos a nova variável que vamos criar no GitHub
BEARER_TOKEN = os.getenv('CARTOLA_BEARER_TOKEN')
GEMINI_KEY = os.getenv('GEMINI_API_KEY')
LIGA_SLUG = "sas-brasil-2026" # <--- AJUSTE AQUI O NOME DA SUA LIGA

# Configurações do BigQuery
# O ID do projeto virá automaticamente do JSON da Service Account
DATASET_ID = "cartola_analytics"
TABELA_HISTORICO = f"{DATASET_ID}.historico"
TABELA_CORNETA = f"{DATASET_ID}.comentarios_ia"

# --- CONFIGURAÇÃO DE CLIENTES ---
def get_bq_client():
    info_chave = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
    credentials = service_account.Credentials.from_service_account_info(info_chave)
    return bigquery.Client(credentials=credentials, project=info_chave['project_id'])

def garantir_infraestrutura(client):
    """Cria dataset e tabelas se não existirem."""
    dataset_id = f"{client.project}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {DATASET_ID} garantido.")
    except Exception as e:
        print(f"Aviso sobre dataset: {e}")

def salvar_bigquery(client, df, tabela_nome):
    table_id = f"{client.project}.{tabela_nome}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Dados salvos em {tabela_nome}")
    except Exception as e:
        print(f"❌ Erro ao salvar no BigQuery: {e}")

# --- COLETA DE DADOS (COM A NOVA AUTENTICAÇÃO) ---
def coletar_dados_liga():
    url = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'x-glb-auth': 'oidc',
        'x-glb-app': 'cartola_web',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }
    
    print("⏳ Coletando dados da liga...")
    res = requests.get(url, headers=headers)
    
    if res.status_code == 200:
        return res.json()
    else:
        print(f"❌ Erro na API Cartola: {res.status_code} - {res.text}")
        return None

# --- INTELIGÊNCIA ARTIFICIAL ---
def gerar_corneta(df_ranking):
    try:
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        lider = df_ranking.iloc[0]['nome']
        vice_lider = df_ranking.iloc[1]['nome']
        lanterna = df_ranking.iloc[-1]['nome']
        vice_lanterna = df_ranking.iloc[-2]['nome']
        pontos_lider = df_ranking.iloc[0]['pontos']
        
        prompt = f"""
        Atue como um comentarista de futebol engraçado e "corneteiro".
        Analise a liga:
        - Líder: {lider} ({pontos_lider} pts).
        - Vice-Líder: {vice_lider}.
        - Vice-Lanterna: {vice_lanterna}.
        - Lanterna: {lanterna}.
        
        Faça um comentário de 2 frases. Elogie o líder mas diga que é sorte, e zoe o lanterna dizendo que ele escalou bagres.
        """
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Erro na IA: {e}")
        return "O comentarista foi demitido (Erro na IA)."

# --- FLUXO PRINCIPAL ---
def main():
    if not BEARER_TOKEN:
        print("Erro: Token do Cartola não encontrado.")
        return

    dados = coletar_dados_liga()
    
    if dados:
        client = get_bq_client()
        garantir_infraestrutura(client)
        
        # Processar Times
        times = dados.get('times', [])
        if not times:
            print("Nenhum time encontrado na liga.")
            return

        ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
        
        # 1. Salvar Histórico
        df = pd.DataFrame(times)[['nome', 'nome_cartola', 'pontos', 'patrimonio']]
        df['timestamp'] = ts_agora
        salvar_bigquery(client, df, TABELA_HISTORICO)
        
        # 2. Salvar Corneta
        ranking = df.sort_values(by='pontos', ascending=False)
        texto_ia = gerar_corneta(ranking)
        df_corneta = pd.DataFrame([{'texto': texto_ia, 'data': ts_agora}])
        salvar_bigquery(client, df_corneta, TABELA_CORNETA)
        
        print("\n🚀 Automação finalizada com sucesso!")

if __name__ == "__main__":
    main()