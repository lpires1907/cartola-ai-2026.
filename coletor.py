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
TIMEOUT = 30 

def get_pro_headers():
    if not TOKEN_SECRET: return None
    t = TOKEN_SECRET.replace("Bearer ", "").strip().strip('"').strip("'")
    return {'authority': 'api.cartola.globo.com', 'authorization': f'Bearer {t}', 'x-glb-app': 'cartola_web', 'x-glb-auth': 'oidc', 'user-agent': 'Mozilla/5.0'}

def get_public_headers():
    return {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

def get_bq_client():
    info = json.loads(GCP_JSON) if isinstance(GCP_JSON, str) else GCP_JSON
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(info), project=info['project_id'])

def limpar_dados_rodada(client, rodada):
    print(f"🧹 Limpando dados da Rodada {rodada} (Parciais e Oficiais)...")
    for t in [TAB_HISTORICO, TAB_ESCALACOES]:
        client.query(f"DELETE FROM `{client.project}.{t}` WHERE rodada = {rodada}").result() # nosec B608

def rodar_coleta():
    client = get_bq_client()
    st = requests.get("https://api.cartola.globo.com/mercado/status", headers=get_public_headers(), timeout=TIMEOUT).json()
    
    r_atual = st.get('rodada_atual', 0)
    status_mercado = st.get('status_mercado') # 1: Aberto, 2: Fechado/Live
    
    # LÓGICA DE TRANSIÇÃO BLINDADA
    if status_mercado == 1: # Mercado Aberto
        # Se aberto, queremos salvar o que FECHOU (Rodada anterior)
        r_alvo = r_atual - 1
        tipo_dado = "OFICIAL"
        is_live = False
    else: # Mercado Fechado / Rodada em andamento
        r_alvo = r_atual
        tipo_dado = "PARCIAL"
        is_live = True

    print(f"🎯 Alvo: Rodada {r_alvo} como {tipo_dado}")

    res_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=get_pro_headers(), timeout=TIMEOUT).json()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_h, l_e = [], []

    for t_obj in res_liga.get('times', []):
        tid = t_obj['time_id']
        res_t = requests.get(f"https://api.cartola.globo.com/time/id/{tid}", headers=get_public_headers(), timeout=TIMEOUT).json()
        
        info_t = res_t.get('time', {})
        v_nome_cartola = info_t.get('nome_cartola') or t_obj.get('nome_cartola') or "Sem Nome"
        v_patrimonio = float(res_t.get('patrimonio') or info_t.get('patrimonio') or 0.0)

        # Se for OFICIAL e a rodada alvo for a que passou, pegamos o ponto fechado
        if tipo_dado == "OFICIAL":
            # Busca especificamente o histórico da rodada que fechou
            hist_res = requests.get(f"https://api.cartola.globo.com/time/id/{tid}/{r_alvo}", headers=get_public_headers(), timeout=TIMEOUT).json()
            pts = float(hist_res.get('pontos', 0.0))
            atletas = hist_res.get('atletas', [])
        else:
            # Se parcial, usa o ponto do objeto de liga (ou cálculo se preferir)
            pts = float(t_obj.get('pontos', {}).get('rodada', 0.0))
            atletas = res_t.get('atletas', [])

        l_h.append({
            'nome': t_obj['nome'], 'nome_cartola': v_nome_cartola, 'pontos': pts, 
            'patrimonio': v_patrimonio, 'rodada': r_alvo, 'timestamp': ts, 'tipo_dado': tipo_dado
        })
        
        for a in atletas:
            l_e.append({
                'rodada': r_alvo, 'liga_time_nome': t_obj['nome'], 
                'atleta_apelido': a.get('apelido'), 
                'pontos': float(a.get('pontos_num', 0.0)), 
                'is_capitao': bool(a.get('atleta_id') == res_t.get('capitao_id')),
                'timestamp': ts
            })
        time.sleep(0.1)

    if l_h:
        limpar_dados_rodada(client, r_alvo) # Apaga o PARCIAL antes de inserir o OFICIAL
        client.load_table_from_dataframe(pd.DataFrame(l_h), f"{client.project}.{TAB_HISTORICO}").result()
        client.load_table_from_dataframe(pd.DataFrame(l_e), f"{client.project}.{TAB_ESCALACOES}").result()
        print(f"✅ Sincronização concluída!")

if __name__ == "__main__":
    rodar_coleta()
