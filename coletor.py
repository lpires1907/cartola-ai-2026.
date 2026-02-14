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
LIGA_SLUG = "sas-brasil-2026"
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_ESCALACOES = f"{DATASET_ID}.times_escalacoes"

GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
TOKEN_SECRET = os.getenv('CARTOLA_GLBID') 

def get_bq_client():
    if not GCP_JSON: return None
    info = json.loads(GCP_JSON) if isinstance(GCP_JSON, str) else GCP_JSON
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

def limpar_dados_rodada_e_futuro(client, rodada_alvo):
    print(f"🧹 Limpando dados da Rodada {rodada_alvo} e fantasmas...")
    for t in [TAB_HISTORICO, TAB_ESCALACOES]:
        query = f"DELETE FROM `{client.project}.{t}` WHERE rodada >= {rodada_alvo}" # nosec B608
        client.query(query).result()

def rodar_coleta():
    client = get_bq_client()
    if not client: return
    
    headers_pub = {'User-Agent': 'Mozilla/5.0'}
    st = requests.get("https://api.cartola.globo.com/mercado/status", headers=headers_pub, timeout=30).json()
    
    r_atual = st.get('rodada_atual', 0)
    # Se mercado está aberto (1), consolidamos a rodada que passou
    r_alvo = (r_atual - 1) if st.get('status_mercado') == 1 else r_atual
    tipo = "OFICIAL" if st.get('status_mercado') == 1 else "PARCIAL"

    token = TOKEN_SECRET.replace("Bearer ", "").strip() if TOKEN_SECRET else ""
    h_pro = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0'}
    
    res_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=h_pro, timeout=30).json()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_h = []

    for t_obj in res_liga.get('times', []):
        tid = t_obj['time_id']
        # Busca detalhes para garantir patrimonio e nome_cartola
        url = f"https://api.cartola.globo.com/time/id/{tid}" + (f"/{r_alvo}" if tipo == "OFICIAL" else "")
        d = requests.get(url, headers=headers_pub, timeout=30).json()
        
        pts = float(d.get('pontos', 0.0)) if tipo == "OFICIAL" else float(t_obj.get('pontos', {}).get('rodada', 0.0))
        
        l_h.append({
            'nome': t_obj['nome'], 'nome_cartola': d.get('time', {}).get('nome_cartola', 'Sem Nome'),
            'pontos': pts, 'patrimonio': float(d.get('patrimonio', 0.0)),
            'rodada': r_alvo, 'timestamp': ts, 'tipo_dado': tipo
        })
        time.sleep(0.1)

    if l_h:
        limpar_dados_rodada_e_futuro(client, r_alvo)
        client.load_table_from_dataframe(pd.DataFrame(l_h), f"{client.project}.{TAB_HISTORICO}").result()
        print(f"✅ Rodada {r_alvo} ({tipo}) sincronizada.")

if __name__ == "__main__":
    rodar_coleta()
