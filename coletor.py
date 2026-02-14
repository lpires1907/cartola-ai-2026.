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
    try:
        info = json.loads(GCP_JSON) if isinstance(GCP_JSON, str) else GCP_JSON
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info['project_id'])
    except: return None

def limpar_dados_rodada_e_futuro(client, rodada_alvo):
    print(f"🧹 Limpando dados da Rodada {rodada_alvo} e futuras...")
    for t in [TAB_HISTORICO, TAB_ESCALACOES]:
        query = f"DELETE FROM `{client.project}.{t}` WHERE rodada >= {rodada_alvo}" # nosec B608
        client.query(query).result()

def rodar_coleta():
    client = get_bq_client()
    if not client: return
    
    headers_pub = {'User-Agent': 'Mozilla/5.0'}
    st = requests.get("https://api.cartola.globo.com/mercado/status", headers=headers_pub, timeout=30).json()
    
    r_atual = st.get('rodada_atual', 0)
    # Se mercado está aberto, o alvo é a rodada ANTERIOR (que fechou)
    mercado_status = st.get('status_mercado')
    r_alvo = (r_atual - 1) if mercado_status == 1 else r_atual
    tipo_dado = "OFICIAL" if mercado_status == 1 else "PARCIAL"

    print(f"🎯 Coleta Iniciada: Rodada {r_alvo} ({tipo_dado})")

    token = TOKEN_SECRET.replace("Bearer ", "").strip() if TOKEN_SECRET else ""
    h_pro = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0'}
    
    res_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=h_pro, timeout=30).json()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_h, l_e = [], []
    pos_map = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}

    for t_obj in res_liga.get('times', []):
        tid = t_obj['time_id']
        
        # --- LÓGICA CRÍTICA PARA RECUPERAR ESCALAÇÃO PASSADA ---
        if tipo_dado == "OFICIAL":
            # Endpoint Histórico (Fundamental para recuperar escalação passada)
            url = f"https://api.cartola.globo.com/time/id/{tid}/{r_alvo}"
            d = requests.get(url, headers=headers_pub, timeout=30).json()
            
            pts = float(d.get('pontos', 0.0))
            atletas = d.get('atletas', [])
            capitao_id = d.get('capitao_id')
        else:
            # Endpoint Ao Vivo
            url = f"https://api.cartola.globo.com/time/id/{tid}"
            d = requests.get(url, headers=headers_pub, timeout=30).json()
            
            pts = float(t_obj.get('pontos', {}).get('rodada', 0.0)) 
            atletas = d.get('atletas', [])
            capitao_id = d.get('capitao_id')

        # Histórico
        l_h.append({
            'nome': t_obj['nome'], 
            'nome_cartola': d.get('time', {}).get('nome_cartola', t_obj.get('nome_cartola')),
            'pontos': pts, 
            'patrimonio': float(d.get('patrimonio', 0.0)),
            'rodada': r_alvo, 
            'timestamp': ts, 
            'tipo_dado': tipo_dado
        })
        
        # Dados da Escalação (Agora preenchido corretamente)
        for a in atletas:
            l_e.append({
                'rodada': r_alvo, 
                'liga_time_nome': t_obj['nome'], 
                'atleta_apelido': a.get('apelido'), 
                'atleta_posicao': pos_map.get(str(a.get('posicao_id')), 'Outro'),
                'pontos': float(a.get('pontos_num', 0.0)), 
                'is_capitao': (a.get('atleta_id') == capitao_id),
                'timestamp': ts
            })
        time.sleep(0.1)

    if l_h:
        limpar_dados_rodada_e_futuro(client, r_alvo)
        client.load_table_from_dataframe(pd.DataFrame(l_h), f"{client.project}.{TAB_HISTORICO}").result()
        client.load_table_from_dataframe(pd.DataFrame(l_e), f"{client.project}.{TAB_ESCALACOES}").result()
        print(f"✅ Rodada {r_alvo} ({tipo_dado}) sincronizada com sucesso!")

if __name__ == "__main__":
    rodar_coleta()
