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

# --- 1. HEADERS ---
def get_pro_headers():
    if not TOKEN_SECRET: return None
    token_limpo = TOKEN_SECRET.replace("Bearer ", "").strip().strip('"').strip("'")
    return {
        'authority': 'api.cartola.globo.com',
        'authorization': f'Bearer {token_limpo}',
        'x-glb-app': 'cartola_web',
        'x-glb-auth': 'oidc',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

def get_public_headers():
    return {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

# --- 2. INFRAESTRUTURA ---
def get_bq_client():
    if not GCP_JSON: raise ValueError("GCP_SERVICE_ACCOUNT ausente.")
    info = json.loads(GCP_JSON) if isinstance(GCP_JSON, str) else GCP_JSON
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

def limpar_dados_rodada(client, rodada):
    sqls = [f"DELETE FROM `{client.project}.{t}` WHERE rodada = {rodada}" for t in [TAB_HISTORICO, TAB_ESCALACOES]]
    for sql in sqls:
        try: client.query(sql).result()
        except: pass

def salvar_bigquery(client, df, tabela, schema):
    if df.empty: return
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION], schema=schema)
    client.load_table_from_dataframe(df, f"{client.project}.{tabela}", job_config=job_config).result()

# --- 3. INTELIGÊNCIA DE PARCIAIS (A MUDANÇA) ---

def buscar_parciais_globais():
    """Baixa as parciais de todos os atletas da rodada atual."""
    url = "https://api.cartola.globo.com/atletas/pontuados"
    try:
        res = requests.get(url, headers=get_public_headers(), timeout=TIMEOUT)
        if res.status_code == 200:
            atletas = res.json().get('atletas', {})
            return {int(id_str): info.get('pontuacao', 0.0) for id_str, info in atletas.items()}
    except: pass
    return {}

def calcular_pontuacao_time(dados_time, mapa_parciais):
    """Soma pontos dos atletas escalados aplicando regra do Capitão."""
    atletas = dados_time.get('atletas', [])
    capitao_id = dados_time.get('capitao_id')
    
    total = 0.0
    detalhes_atletas = []
    
    for atl in atletas:
        aid = atl.get('atleta_id')
        # Busca parcial real no mapa global
        pts = mapa_parciais.get(aid, 0.0)
        
        is_cap = (aid == capitao_id)
        pts_finais = pts * 1.5 if is_cap else pts
        
        total += pts_finais
        detalhes_atletas.append({
            'apelido': atl.get('apelido'),
            'posicao_id': atl.get('posicao_id'),
            'pontos': round(pts, 2),
            'is_capitao': is_cap,
            'atleta_id': aid
        })
        
    return round(total, 2), detalhes_atletas

# --- 4. EXECUÇÃO ---
def rodar_coleta():
    client = get_bq_client()
    
    # 1. Status do Mercado
    status_api = requests.get("https://api.cartola.globo.com/mercado/status", headers=get_public_headers(), timeout=TIMEOUT).json()
    rodada_alvo = status_api.get('rodada_atual', 0)
    is_live = (status_api.get('status_mercado') == 2)
    tipo_dado = "PARCIAL" if is_live else "OFICIAL"
    
    print(f"🔄 Rodada Alvo: {rodada_alvo} ({tipo_dado})")

    # 2. Busca Parciais Globais se estiver em jogo
    mapa_parciais = buscar_parciais_globais() if is_live else {}
    if mapa_parciais: print(f"📡 {len(mapa_parciais)} parciais de atletas carregadas.")

    # 3. Busca a Liga
    url_liga = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    res_liga = requests.get(url_liga, headers=get_pro_headers(), timeout=TIMEOUT)
    
    if res_liga.status_code != 200:
        print(f"❌ Erro ao acessar liga: {res_liga.status_code}")
        return

    times_liga = res_liga.json().get('times', [])
    print(f"🚀 Recalculando pontos para {len(times_liga)} times...")

    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_hist, l_esc = [], []
    posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}

    for t_obj in times_liga:
        tid = t_obj['time_id']
        nome_time = t_obj['nome']
        
        # Busca escalação
        url_time = f"https://api.cartola.globo.com/time/id/{tid}"
        res_t = requests.get(url_time, headers=get_public_headers(), timeout=TIMEOUT)
        
        if res_t.status_code == 200:
            dados_time = res_t.json()
            
            # Recálculo Real-Time (se live) ou usa o da API (se oficial)
            if is_live:
                pts_total, atletas_calculados = calcular_pontuacao_time(dados_time, mapa_parciais)
            else:
                pts_total = t_obj.get('pontos', {}).get('rodada', 0.0)
                atletas_calculados = [{'apelido': a['apelido'], 'posicao_id': a['posicao_id'], 'pontos': a.get('pontos_num', 0.0), 'is_capitao': (a['atleta_id'] == dados_time.get('capitao_id'))} for a in dados_time.get('atletas', [])]

            l_hist.append({
                'nome': nome_time, 'pontos': float(pts_total), 'rodada': int(rodada_alvo), 
                'timestamp': ts_agora, 'tipo_dado': tipo_dado
            })

            for a in atletas_calculados:
                l_esc.append({
                    'rodada': int(rodada_alvo), 'liga_time_nome': nome_time,
                    'atleta_apelido': a['apelido'], 'atleta_posicao': posicoes.get(str(a['posicao_id']), ''),
                    'pontos': float(a['pontos']), 'is_capitao': a['is_capitao'],
                    'timestamp': ts_agora
                })
        time.sleep(0.2)

    if l_hist:
        limpar_dados_rodada(client, rodada_alvo)
        salvar_bigquery(client, pd.DataFrame(l_hist), TAB_HISTORICO, None)
        salvar_bigquery(client, pd.DataFrame(l_esc), TAB_ESCALACOES, None)
        print("✅ Dados de parciais salvos com sucesso!")

if __name__ == "__main__":
    rodar_coleta()
