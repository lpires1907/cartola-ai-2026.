import os
import json
import requests
import pandas as pd
import google.generativeai as genai
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES GERAIS ---
# Mude aqui para o slug da sua liga
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

def garantir_tabelas(client):
    dataset_ref = f"{client.project}.{DATASET_ID}"
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    try:
        client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {DATASET_ID} garantido.")
    except Exception as e:
        print(f"Aviso dataset: {e}")

def salvar_bigquery(client, df, tabela_nome):
    table_id = f"{client.project}.{tabela_nome}"
    # Schema forçado para garantir que 'pontos' seja FLOAT
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("nome", "STRING"),
            bigquery.SchemaField("nome_cartola", "STRING"),
            bigquery.SchemaField("pontos", "FLOAT"),
            bigquery.SchemaField("patrimonio", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]
    )
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Sucesso: Dados salvos em {tabela_nome}")
    except Exception as e:
        print(f"❌ Erro BigQuery: {e}")

# --- TRATAMENTO DE DADOS (A CORREÇÃO ESTÁ AQUI) ---
def extrair_pontuacao(dado_pontos):
    """
    Função para lidar com a bagunça da API.
    Se vier um número, usa o número.
    Se vier um dicionário {'rodada': None...}, tenta pegar o campeonato ou retorna 0.0.
    """
    if isinstance(dado_pontos, (int, float)):
        return float(dado_pontos)
    
    if isinstance(dado_pontos, dict):
        # Tenta pegar a pontuação do campeonato, se for None, vira 0.0
        return float(dado_pontos.get('campeonato') or 0.0)
    
    # Se for None ou qualquer outra coisa
    return 0.0

# --- COLETA ---
def coletar_dados():
    url = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'x-glb-auth': 'oidc',
        'x-glb-app': 'cartola_web',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }
    
    print(f"🔍 Buscando dados da liga...")
    res = requests.get(url, headers=headers)
    
    if res.status_code == 200:
        return res.json()
    else:
        print(f"❌ Erro API Cartola ({res.status_code}): {res.text}")
        return None

# --- IA (GEMINI) ---
def gerar_analise_ia(df_ranking):
    if not GEMINI_KEY:
        return "IA indisponível (Chave não configurada)."
        
    try:
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        lider = df_ranking.iloc[0]['nome']
        lanterna = df_ranking.iloc[-1]['nome']
        pontos_lider = df_ranking.iloc[0]['pontos']
        
        prompt = f"""
        Você é um narrador esportivo brasileiro muito sarcástico.
        Resuma a rodada desta liga de Cartola:
        - Líder: {lider} ({pontos_lider} pts).
        - Lanterna: {lanterna}.
        
        Faça uma piada curta (max 200 caracteres) elogiando a sorte do líder e zoando o lanterna.
        """
        response = model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Erro Gemini: {e}")
        return "A IA foi para o departamento médico."

# --- MAIN ---
def main():
    if not BEARER_TOKEN:
        print("⛔ ERRO: Secret CARTOLA_BEARER_TOKEN não encontrada.")
        return

    dados = coletar_dados()
    if not dados: return

    times = dados.get('times', [])
    if not times:
        print("Nenhum time encontrado.")
        return

    # Preparar Dados COM TRATAMENTO
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    
    lista_limpa = []
    for time in times:
        # Aqui aplicamos a correção para extrair o número real
        pontos_reais = extrair_pontuacao(time.get('pontos'))
        patrimonio_real = float(time.get('patrimonio', 100))
        
        lista_limpa.append({
            'nome': time['nome'],
            'nome_cartola': time['nome_cartola'],
            'pontos': pontos_reais,
            'patrimonio': patrimonio_real,
            'timestamp': ts_agora
        })

    # Cria o DataFrame já limpo e numérico
    df = pd.DataFrame(lista_limpa)

    client = get_bq_client()
    garantir_tabelas(client)
    
    # 1. Salvar Histórico
    salvar_bigquery(client, df, TABELA_HISTORICO)
    
    # 2. Gerar Corneta
    ranking = df.sort_values(by='pontos', ascending=False)
    texto = gerar_analise_ia(ranking)
    df_corneta = pd.DataFrame([{'texto': texto, 'data': ts_agora}])
    salvar_bigquery(client, df_corneta, TABELA_CORNETA)
    
    print("\n🚀 Processo finalizado com sucesso!")

if __name__ == "__main__":
    main()