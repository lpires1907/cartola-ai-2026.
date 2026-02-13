import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz
import time

# --- CONFIGURAÇÕES ---
ARQUIVO_CONFIG = "copas.json"
DATASET_ID = "cartola_analytics"
TAB_COPA = f"{DATASET_ID}.copa_mata_mata"

def get_bq_client():
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT')) if os.getenv('GCP_SERVICE_ACCOUNT') else None
    if info:
        return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(info), project=info['project_id'])
    return bigquery.Client()

def caçar_jogos_recursivo(dados):
    jogos = []
    if isinstance(dados, dict):
        if 'time_mandante_id' in dados: return [dados]
        for v in dados.values(): jogos.extend(caçar_jogos_recursivo(v))
    elif isinstance(dados, list):
        for item in dados: jogos.extend(caçar_jogos_recursivo(item))
    return jogos

def coletar_dados_copa():
    if not os.path.exists(ARQUIVO_CONFIG): return
    with open(ARQUIVO_CONFIG, 'r', encoding='utf-8') as f:
        copas = json.load(f)
        
    token = os.getenv("CARTOLA_GLBID")
    headers = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0'}
    client = get_bq_client()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))

    for copa in copas:
        if not copa.get('ativa'): continue
        try:
            res = requests.get(f"https://api.cartola.globo.com/auth/liga/{copa['slug']}", headers=headers, timeout=30).json()
            jogos = caçar_jogos_recursivo(res.get('chaves_mata_mata', {}))
            
            if jogos:
                # SÓ LIMPA SE TIVER JOGOS PARA INSERIR
                client.query(f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{copa['slug']}'").result() # nosec B608
                
                l_final = []
                for j in jogos:
                    l_final.append({
                        'nome_copa': copa['nome_visual'], 'fase_copa': j.get('tipo_fase', 'Fase'),
                        'time_a_nome': f"Time {j.get('time_mandante_id')}", 'time_a_pontos': float(j.get('time_mandante_pontuacao') or 0.0),
                        'time_b_nome': f"Time {j.get('time_visitante_id')}", 'time_b_pontos': float(j.get('time_visitante_pontuacao') or 0.0),
                        'liga_slug': copa['slug'], 'data_coleta': ts
                    })
                client.load_table_from_dataframe(pd.DataFrame(l_final), TAB_COPA).result()
                print(f"✅ Copa {copa['slug']} atualizada.")
        except: pass

if __name__ == "__main__":
    coletar_dados_copa()
