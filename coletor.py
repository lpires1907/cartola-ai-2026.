import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz
import time

LIGA_SLUG = "sas-brasil-2026"
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_ESCALACOES = f"{DATASET_ID}.times_escalacoes"
GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
TOKEN_SECRET = os.getenv('CARTOLA_GLBID') 

def get_bq_client():
    if not GCP_JSON: return None
    try:
        info = json.loads(GCP_JSON) if isinstance(GCP_JSON, str) else GCP_JSON
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info['project_id'])
    except: return None

def limpar_dados_rodada_e_futuro(client, rodada_alvo):
    print(f"🧹 Limpando rodada {rodada_alvo} e futuras...")
    for t in [TAB_HISTORICO, TAB_ESCALACOES]:
        query = f"DELETE FROM `{client.project}.{t}` WHERE rodada >= {rodada_alvo}" # nosec B608
        client.query(query).result()

def rodar_coleta():
    client = get_bq_client()
    if not client: return
    
    headers_pub = {'User-Agent': 'Mozilla/5.0'}
    st = requests.get("https://api.cartola.globo.com/mercado/status", headers=headers_pub, timeout=30).json()
    
    # Lógica de Mercado Aberto = Rodada Passada Oficial
    r_alvo = (st.get('rodada_atual') - 1) if st.get('status_mercado') == 1 else st.get('rodada_atual')
    tipo_dado = "OFICIAL" if st.get('status_mercado') == 1 else "PARCIAL"

    print(f"🎯 Coleta Iniciada: Rodada {r_alvo} ({tipo_dado})")

    token = TOKEN_SECRET.replace("Bearer ", "").strip() if TOKEN_SECRET else ""
    h_pro = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0'}
    res_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=h_pro, timeout=30).json()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_h, l_e = [], []
    pos_map = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}

    for t_obj in res_liga.get('times', []):
        tid = t_obj['time_id']
        # Se OFICIAL, busca no histórico para recuperar escalação e ponto final
        url = f"https://api.cartola.globo.com/time/id/{tid}/{r_alvo}" if tipo_dado == "OFICIAL" else f"https://api.cartola.globo.com/time/id/{tid}"
        d = requests.get(url, headers=headers_pub, timeout=30).json()
        
        # Pontos e Atletas
        if tipo_dado == "OFICIAL":
            pts = float(d.get('pontos', 0.0))
        else:
            pts = float(t_obj.get('pontos', {}).get('rodada', 0.0))
            
        l_h.append({
            'nome': t_obj['nome'], 
            'nome_cartola': d.get('time', {}).get('nome_cartola', 'Sem Nome'),
            'pontos': pts, 
            'patrimonio': float(d.get('patrimonio', 0.0)),
            'rodada': r_alvo, 
            'timestamp': ts, 
            'tipo_dado': tipo_dado
        })
        
        for a in d.get('atletas', []):
            l_e.append({
                'rodada': r_alvo, 
                'liga_time_nome': t_obj['nome'], 
                'atleta_apelido': a.get('apelido'), 
                'atleta_posicao': pos_map.get(str(a.get('posicao_id')), 'Outro'),
                'pontos': float(a.get('pontos_num', 0.0)), 
                'is_capitao': (a.get('atleta_id') == d.get('capitao_id')),
                'timestamp': ts
            })
        time.sleep(0.1)

    if l_h:
        limpar_dados_rodada_e_futuro(client, r_alvo)
        client.load_table_from_dataframe(pd.DataFrame(l_h), f"{client.project}.{TAB_HISTORICO}").result()
        client.load_table_from_dataframe(pd.DataFrame(l_e), f"{client.project}.{TAB_ESCALACOES}").result()
        print(f"✅ Rodada {r_alvo} sincronizada!")

if __name__ == "__main__":
    rodar_coleta()
