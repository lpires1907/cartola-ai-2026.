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
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=schema 
    )
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Sucesso: {len(df)} linhas salvas em {tabela_nome}")
    except Exception as e:
        print(f"❌ Erro BigQuery ({tabela_nome}): {e}")
        # Imprime o erro detalhado se houver
        if hasattr(e, 'errors'):
            print(f"Detalhes do erro: {e.errors}")

# --- TRATAMENTO DE DADOS (CORRIGIDO) ---
def extrair_detalhes(dado_pontos, liga_rodada_atual):
    """
    Retorna uma tupla: (pontos_campeonato, numero_rodada)
    """
    rodada_numero = int(liga_rodada_atual)
    pontos_total = 0.0

    # Se for None, retorna zero e a rodada oficial da liga
    if dado_pontos is None:
        return pontos_total, rodada_numero

    # Caso 1: Dado é um dicionário (Padrão atual)
    if isinstance(dado_pontos, dict):
        # 'campeonato': Pontuação total acumulada
        # 'rodada': Pontuação DA rodada (Cuidado! Isso não é o ID da rodada)
        
        val_campeonato = dado_pontos.get('campeonato')
        
        if val_campeonato is not None:
            pontos_total = float(val_campeonato)
        else:
            # Fallback: Tenta pegar 'rodada' se campeonato for nulo (início de camp)
            val_rodada = dado_pontos.get('rodada')
            if val_rodada is not None:
                pontos_total = float(val_rodada)
                
        return pontos_total, rodada_numero
        
    # Caso 2: Dado é um número direto (fallback antigo)
    if isinstance(dado_pontos, (int, float)):
        return float(dado_pontos), rodada_numero
        
    return pontos_total, rodada_numero

# --- COLETA ---
def coletar_dados():
    url = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    headers = {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'x-glb-auth': 'oidc',
        'x-glb-app': 'cartola_web',
        'User-Agent': 'Mozilla/5.0',
    }
    
    print(f"🔍 Buscando dados da liga: {LIGA_SLUG}...")
    res = requests.get(url, headers=headers)
    
    if res.status_code == 200:
        return res.json()
    else:
        print(f"❌ Erro API Cartola ({res.status_code}): {res.text}")
        return None

# --- IA (GEMINI 1.5 FLASH) ---
def gerar_analise_ia(df_ranking, rodada_atual):
    if not GEMINI_KEY:
        print("Chave Gemini não encontrada.")
        return "IA indisponível."
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_KEY}"
    
    qtd_times = len(df_ranking)
    lider = df_ranking.iloc[0]
    lanterna = df_ranking.iloc[-1]
    
    info_vice_lider = ""
    info_vice_lanterna = ""
    
    if qtd_times >= 2:
        vice = df_ranking.iloc[1]
        info_vice_lider = f"- Vice-Líder: {vice['nome']} ({vice['pontos']} pts)."
    
    if qtd_times >= 3:
        v_lanterna = df_ranking.iloc[-2]
        info_vice_lanterna = f"- Vice-Lanterna: {v_lanterna['nome']} ({v_lanterna['pontos']} pts)."

    prompt_texto = f"""
    Você é um narrador esportivo sarcástico.
    Estamos na RODADA {rodada_atual} da liga Cartola "{LIGA_SLUG}".
    
    Resumo:
    - Líder: {lider['nome']} ({lider['pontos']} pts).
    {info_vice_lider}
    {info_vice_lanterna}
    - Lanterna: {lanterna['nome']} ({lanterna['pontos']} pts).
    
    Faça um comentário ácido e engraçado (max 280 caracteres).
    Zoe o Lanterna e diga que o Líder está com sorte.
    """

    payload = {"contents": [{"parts": [{"text": prompt_texto}]}]}
    headers = {'Content-Type': 'application/json'}

    try:
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code == 200:
            resultado = response.json()
            return resultado['candidates'][0]['content']['parts'][0]['text']
        else:
            print(f"❌ Erro API Gemini ({response.status_code}): {response.text}")
            return "Narrador sem sinal."
    except Exception as e:
        print(f"❌ Erro Conexão Gemini: {e}")
        return "Narrador fora do ar."

# --- FLUXO PRINCIPAL ---
def main():
    if not BEARER_TOKEN:
        print("⛔ ERRO: Token não encontrado.")
        return

    dados = coletar_dados()
    if not dados: return

    # --- DEBUG: IMPRIMIR ESTRUTURA REAL ---
    # Isso vai mostrar no log exatamente o que o Cartola está mandando
    print("-" * 30)
    print(f"DEBUG ESTRUTURA JSON (Rodada Geral: {dados.get('rodada_atual')})")
    if 'times' in dados and len(dados['times']) > 0:
        exemplo = dados['times'][0]
        print(f"Exemplo Time 1: {exemplo.get('nome')}")
        print(f"Dados Pontos Time 1: {exemplo.get('pontos')}")
        print("-" * 30)
    # --------------------------------------

    rodada_geral = dados.get('rodada_atual', 0)
    times = dados.get('times', [])
    
    if not times:
        print("Nenhum time encontrado.")
        return

    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    lista_limpa = []

    for time in times:
        pontos, rodada = extrair_detalhes(time.get('pontos'), rodada_geral)
        
        lista_limpa.append({
            'nome': str(time['nome']),
            'nome_cartola': str(time['nome_cartola']),
            'pontos': float(pontos),
            'rodada': int(rodada),
            'patrimonio': float(time.get('patrimonio', 100)),
            'timestamp': ts_agora
        })

    df_historico = pd.DataFrame(lista_limpa)
    
    # DEBUG: Mostrar o que será salvo
    print(f"📊 Preparando para salvar {len(df_historico)} registros.")
    print(f"Média de pontos calculada: {df_historico['pontos'].mean()}")

    schema_historico = [
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("nome_cartola", "STRING"),
        bigquery.SchemaField("pontos", "FLOAT"),
        bigquery.SchemaField("rodada", "INTEGER"),
        bigquery.SchemaField("patrimonio", "FLOAT"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
    ]

    client = get_bq_client()
    garantir_dataset(client)
    
    salvar_bigquery(client, df_historico, TABELA_HISTORICO, schema_historico)
    
    # IA
    ranking = df_historico.sort_values(by='pontos', ascending=False)
    print(f"🤖 Gerando comentário da Rodada {rodada_geral}...")
    texto_ia = gerar_analise_ia(ranking, rodada_geral)
    
    df_corneta = pd.DataFrame([{
        'texto': str(texto_ia), 
        'rodada': int(rodada_geral),
        'data': ts_agora
    }])
    
    schema_corneta = [
        bigquery.SchemaField("texto", "STRING"),
        bigquery.SchemaField("rodada", "INTEGER"),
        bigquery.SchemaField("data", "TIMESTAMP"),
    ]
    
    salvar_bigquery(client, df_corneta, TABELA_CORNETA, schema_corneta)
    
    print("\n🚀 Automação concluída!")

if __name__ == "__main__":
    main()