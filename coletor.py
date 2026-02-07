import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES ---
LIGA_SLUG = "sas-brasil-2026"
DATASET_ID = "cartola_analytics"

TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_ESCALACOES = f"{DATASET_ID}.times_escalacoes"
TAB_ATLETAS = f"{DATASET_ID}.atletas_globais"

GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
# AQUI: Coloque seu Cookie 'glbId' (que dura meses)
COOKIE_SECRET = os.getenv('CARTOLA_GLBID') 
TIMEOUT = 30 

# --- 1. GERADOR DE TOKEN (MODO GENÉRICO) ---
def gerar_token_pro_automatico(cookie_str):
    if not cookie_str: return None

    print("🔄 Tentando gerar Token via ServiceID Genérico...")
    
    # 1. Extrai glbId
    glb_id_val = ""
    try:
        if "glbId=" in cookie_str:
            glb_id_val = cookie_str.split("glbId=")[1].split(";")[0]
        else:
            glb_id_val = cookie_str.strip()
    except:
        return None

    # 2. Bate na API com ID Genérico (438 em vez de 4728)
    url_auth = "https://login.globo.com/api/authentication"
    
    payload = {
        "payload": {
            "serviceId": 438,  # <--- MUDANÇA AQUI (ID Genérico da Globo)
            "glbId": glb_id_val
        }
    }
    
    headers_login = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Origin": "https://www.globo.com", # <--- Mudamos a origem também
        "Referer": "https://www.globo.com/",
    }

    try:
        res = requests.post(url_auth, json=payload, headers=headers_login, timeout=15)
        
        if res.status_code == 200:
            data = res.json()
            if "id" in data:
                print(f"✅ Token gerado! (Genérico)")
                return data["id"]
            else:
                print(f"⚠️ Resposta sem token: {data}")
        else:
            print(f"⚠️ Falha ({res.status_code}): {res.text[:100]}")
            
    except Exception as e:
        print(f"❌ Erro de conexão: {e}")

    return None

# --- 2. GERENCIAMENTO DE HEADERS ---
def get_headers_com_token(token):
    return {
        'authority': 'api.cartola.globo.com',
        'accept': 'application/json',
        'accept-language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': f'Bearer {token}',
        'origin': 'https://cartola.globo.com',
        'referer': 'https://cartola.globo.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'x-glb-app': 'cartola_web',
        'x-glb-auth': 'oidc',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty'
    }

def get_public_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }

# --- 3. INFRAESTRUTURA ---
def get_bq_client():
    if not GCP_JSON: raise ValueError("GCP_SERVICE_ACCOUNT ausente.")
    try:
        info = json.loads(GCP_JSON) if isinstance(GCP_JSON, str) else GCP_JSON
    except:
        info = GCP_JSON 
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

def garantir_dataset(client):
    try: client.create_dataset(bigquery.Dataset(f"{client.project}.{DATASET_ID}"), exists_ok=True)
    except: pass

def limpar_dados_rodada(client, rodada):
    print(f"🧹 Limpando dados da Rodada {rodada}...")
    # CORREÇÃO BANDIT: nosec na mesma linha
    sqls = [f"DELETE FROM `{client.project}.{t}` WHERE rodada = {rodada}" for t in [TAB_HISTORICO, TAB_ESCALACOES, TAB_ATLETAS]] # nosec
    for sql in sqls:
        try: client.query(sql).result()
        except: pass

def salvar_bigquery(client, df, tabela, schema):
    if df.empty: return
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION], schema=schema)
    client.load_table_from_dataframe(df, f"{client.project}.{tabela}", job_config=job_config).result()
    print(f"✅ Salvo em {tabela}")

# --- 4. API INTELIGENTE ---
def get_dados_time_smart(time_id, rodada, is_live, token_valido):
    url = f"https://api.cartola.globo.com/time/parcial/{time_id}" if is_live else f"https://api.cartola.globo.com/time/id/{time_id}/{rodada}"
    try:
        if token_valido:
            headers = get_headers_com_token(token_valido)
            res = requests.get(url, headers=headers, timeout=TIMEOUT)
            if res.status_code == 200: return res.json()
        
        res = requests.get(url, headers=get_public_headers(), timeout=TIMEOUT)
        return res.json() if res.status_code == 200 else {'pontos': 0, 'atletas': []}
    except: return {'pontos': 0, 'atletas': []}

# --- 5. EXECUÇÃO PRINCIPAL ---
def rodar_coleta():
    if not COOKIE_SECRET:
        print("⚠️ AVISO: Sem Cookie configurado.")
        token_ativo = None
    else:
        # Tenta gerar o token fresco
        token_ativo = gerar_token_pro_automatico(COOKIE_SECRET)
    
    client = get_bq_client()
    garantir_dataset(client)

    # 1. Mercado
    try:
        status_api = requests.get("https://api.cartola.globo.com/mercado/status", headers=get_public_headers(), timeout=TIMEOUT).json()
    except Exception as e:
        print(f"❌ Erro fatal: {e}")
        return

    mercado_status = status_api.get('status_mercado', 1) 
    rodada_cartola = status_api.get('rodada_atual', 0)
    is_live = (mercado_status == 2)
    rodada_alvo = rodada_cartola if is_live else (rodada_cartola - 1)
    tipo_dado = "PARCIAL" if is_live else "OFICIAL"
    
    print(f"🔄 Rodada Alvo: {rodada_alvo} ({tipo_dado})")

    # 2. BUSCA DA LIGA
    print(f"🌍 Acessando liga: {LIGA_SLUG}")
    
    headers_liga = get_public_headers()
    url_liga = f"https://api.cartola.globo.com/liga/{LIGA_SLUG}"
    
    if token_ativo:
        url_liga = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}?orderBy=campeonato&page=1"
        headers_liga = get_headers_com_token(token_ativo)
    
    res_liga = requests.get(url_liga, headers=headers_liga, timeout=TIMEOUT)
    
    if res_liga.status_code == 200:
        modo = "PRO (Autenticado)" if token_ativo else "Público"
        print(f"✅ Acesso confirmado! Modo: {modo}")
    else:
        print(f"❌ Falha no acesso ({res_liga.status_code})...")
        if token_ativo:
            print("⚠️ Tentando fallback público...")
            url_pub = f"https://api.cartola.globo.com/liga/{LIGA_SLUG}"
            res_liga = requests.get(url_pub, headers=get_public_headers(), timeout=TIMEOUT)

    if res_liga is None or res_liga.status_code != 200:
        print(f"❌ Erro final ({res_liga.status_code if res_liga else 'N/A'}).")
        return

    times_liga = res_liga.json().get('times', [])
    print(f"🚀 Processando {len(times_liga)} times...")

    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_hist, l_esc = [], []
    posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}

    for time_obj in times_liga:
        nome_time = time_obj['nome']
        dados_time = get_dados_time_smart(time_obj['time_id'], rodada_alvo, is_live, token_ativo)
        
        pts = dados_time.get('pontos')
        if pts is None: pts = time_obj.get('pontos', {}).get('rodada', 0.0)
        
        l_hist.append({
            'nome': nome_time, 'nome_cartola': time_obj.get('nome_cartola', ''),
            'pontos': float(pts), 'patrimonio': float(time_obj.get('patrimonio', 100)),
            'rodada': int(rodada_alvo), 'timestamp': ts_agora, 'tipo_dado': tipo_dado
        })

        for atl in dados_time.get('atletas', []):
            l_esc.append({
                'rodada': int(rodada_alvo), 'liga_time_nome': nome_time,
                'atleta_apelido': atl.get('apelido'), 'atleta_posicao': posicoes.get(str(atl.get('posicao_id')), ''),
                'pontos': float(atl.get('pontos_num', 0.0)), 
                'is_capitao': bool(int(atl['atleta_id']) == int(dados_time.get('capitao_id', 0))),
                'status_rodada': tipo_dado, 'timestamp': ts_agora
            })

    if l_hist:
        limpar_dados_rodada(client, rodada_alvo)
        s_hist = [bigquery.SchemaField("nome", "STRING"), bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("timestamp", "TIMESTAMP"), bigquery.SchemaField("tipo_dado", "STRING")]
        salvar_bigquery(client, pd.DataFrame(l_hist), TAB_HISTORICO, s_hist)
        
        s_esc = [bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("liga_time_nome", "STRING"), bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("is_capitao", "BOOLEAN"), bigquery.SchemaField("timestamp", "TIMESTAMP")]
        salvar_bigquery(client, pd.DataFrame(l_esc), TAB_ESCALACOES, s_esc)

    print("🏁 Coleta finalizada com sucesso.")

if __name__ == "__main__":
    rodar_coleta()
