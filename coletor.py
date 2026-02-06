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

# Variáveis globais
GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
# Esta variável agora deve conter a STRING DE COOKIE COMPLETA copiada do navegador
COOKIE_SECRET = os.getenv('CARTOLA_GLBID') 
TIMEOUT = 30 

# --- 1. GERENCIAMENTO DE HEADERS INTELIGENTE ---
def get_auth_headers():
    """
    Constrói os headers de autenticação baseados no Cookie completo.
    O segredo CARTOLA_GLBID agora deve ter a string inteira do cookie.
    """
    if not COOKIE_SECRET:
        return None

    # Tenta extrair o valor puro do glbId de dentro da string de cookies
    # O header X-GLB-Token geralmente precisa ser apenas o valor do glbId
    x_glb_token = ""
    try:
        # Procura por 'glbId=Valor;' ou 'glbId=Valor' no final
        if "glbId=" in COOKIE_SECRET:
            parts = COOKIE_SECRET.split('glbId=')[1]
            x_glb_token = parts.split(';')[0]
    except:
        pass

    # Se não achou glbId na string, usa a string toda (caso o usuário tenha colado só o ID)
    if not x_glb_token:
        x_glb_token = COOKIE_SECRET

    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json',
        'Cookie': COOKIE_SECRET,     # Envia TODOS os cookies (glbId, glb_uid_jwt, etc)
        'X-GLB-Token': x_glb_token,  # Envia o ID da sessão como token
        'Referer': 'https://cartola.globo.com/',
        'Origin': 'https://cartola.globo.com'
    }

def get_public_headers():
    return {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    }

# --- 2. INFRAESTRUTURA ---
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
    # nosec: Lista controlada
    sqls = [f"DELETE FROM `{client.project}.{t}` WHERE rodada = {rodada}" for t in [TAB_HISTORICO, TAB_ESCALACOES, TAB_ATLETAS]] # nosec
    for sql in sqls:
        try: client.query(sql).result()
        except: pass

def salvar_bigquery(client, df, tabela, schema):
    if df.empty: return
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION], schema=schema)
    client.load_table_from_dataframe(df, f"{client.project}.{tabela}", job_config=job_config).result()
    print(f"✅ Salvo em {tabela}")

# --- 3. API INTELIGENTE ---
def get_dados_time_smart(time_id, rodada, is_live):
    url = f"https://api.cartola.globo.com/time/parcial/{time_id}" if is_live else f"https://api.cartola.globo.com/time/id/{time_id}/{rodada}"
    try:
        # Tenta rota PRO (com cookies completos)
        auth_h = get_auth_headers()
        if auth_h:
            res = requests.get(url, headers=auth_h, timeout=TIMEOUT)
            if res.status_code == 200: return res.json()
        
        # Fallback Público (sem cookies)
        res = requests.get(url, headers=get_public_headers(), timeout=TIMEOUT)
        return res.json() if res.status_code == 200 else {'pontos': 0, 'atletas': []}
    except: return {'pontos': 0, 'atletas': []}

# --- 4. EXECUÇÃO PRINCIPAL ---
def rodar_coleta():
    if not COOKIE_SECRET:
        print("⚠️ AVISO: Variável CARTOLA_GLBID vazia.")
    else:
        print("🍪 Cookies carregados via Secrets.")
    
    client = get_bq_client()
    garantir_dataset(client)

    # 1. Mercado
    try:
        status_api = requests.get("https://api.cartola.globo.com/mercado/status", headers=get_public_headers(), timeout=TIMEOUT).json()
    except Exception as e:
        print(f"❌ Erro fatal ao checar mercado: {e}")
        return

    mercado_status = status_api.get('status_mercado', 1) 
    rodada_cartola = status_api.get('rodada_atual', 0)
    game_over = status_api.get('game_over', False)
    
    is_live = (mercado_status == 2)
    tipo_dado = "PREVIA" if (is_live and game_over) else ("PARCIAL" if is_live else "OFICIAL")
    rodada_alvo = rodada_cartola if is_live else (rodada_cartola - 1)

    print(f"🔄 Rodada Alvo: {rodada_alvo} ({tipo_dado})")

    # 2. Metadados
    try:
        res_clubes = requests.get("https://api.cartola.globo.com/clubes", headers=get_public_headers(), timeout=TIMEOUT).json()
        posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}
    except: posicoes = {}

    # 3. BUSCA DA LIGA
    print(f"🌍 Tentando acessar dados da liga: {LIGA_SLUG}")
    
    # Rota PRO (Autenticada)
    url_auth = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    res_liga = requests.get(url_auth, headers=get_auth_headers(), timeout=TIMEOUT)
    
    if res_liga.status_code == 200:
        print("✅ Acesso via rota PRO (/auth/liga) funcionou!")
    else:
        print(f"⚠️ Rota PRO falhou ({res_liga.status_code}). O Cookie pode estar incompleto ou expirado.")
        # Se a autenticação falhar para uma liga privada, a pública também falhará (dá 500 ou 404).
        # Mas tentamos mesmo assim caso a liga tenha se tornado pública.
        url_pub = f"https://api.cartola.globo.com/liga/{LIGA_SLUG}"
        res_liga = requests.get(url_pub, headers=get_public_headers(), timeout=TIMEOUT)

    if res_liga.status_code != 200:
        print(f"❌ Erro final ao acessar liga: {res_liga.status_code}")
        return

    times_liga = res_liga.json().get('times', [])
    if not times_liga:
        print(f"⚠️ A liga foi acessada mas não retornou times.")
        return

    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_hist, l_esc = [], []

    print(f"🚀 Processando {len(times_liga)} times...")

    for time_obj in times_liga:
        nome_time = time_obj['nome']
        dados_time = get_dados_time_smart(time_obj['time_id'], rodada_alvo, is_live)
        
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
