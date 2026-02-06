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
BEARER_TOKEN = None
GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
GLBID_SECRET = os.getenv('CARTOLA_GLBID') # Pega o cookie direto do ambiente
TIMEOUT = 30 

# --- 1. AUTENTICAÇÃO E HEADERS ---
def get_headers():
    """
    Gera os headers de autenticação.
    Prioridade 1: Bearer Token (se existir)
    Prioridade 2: Cookie glbId (se existir)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
    
    if BEARER_TOKEN:
        headers['Authorization'] = f'Bearer {BEARER_TOKEN}'
        headers['x-glb-auth'] = 'oidc'
    elif GLBID_SECRET:
        # AQUI ESTÁ O PULO DO GATO: Usa o cookie direto se não tiver token
        headers['Cookie'] = f'glbId={GLBID_SECRET}'
        
    return headers

def buscar_token_automatico():
    """
    Tenta gerar um Bearer Token novo. 
    Se falhar (404/406), retorna None, mas o código seguirá usando o Cookie via get_headers().
    """
    # 1. Tenta renovar usando o Cookie (Troca Ticket)
    if GLBID_SECRET:
        print("🍪 Tentando renovar Token usando Cookie GLBID...")
        try:
            headers_cookie = {
                'Cookie': f'glbId={GLBID_SECRET}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
            }
            # Tenta endpoint padrão
            res = requests.get("https://api.cartola.globo.com/auth/token", headers=headers_cookie, timeout=TIMEOUT)
            
            if res.status_code == 200:
                print("✅ Token Bearer gerado com sucesso!")
                return res.json().get('token')
            else:
                print(f"⚠️ API de Token retornou {res.status_code}. Seguiremos usando o Cookie direto.")
                return None
        except Exception as e:
            print(f"⚠️ Erro ao tentar renovar token: {e}. Seguiremos usando o Cookie direto.")
            return None

    # 2. Login com User/Senha (Último caso - geralmente falha com 406 no Actions)
    email = os.getenv('CARTOLA_EMAIL')
    senha = os.getenv('CARTOLA_SENHA')
    
    if email and senha:
        print("🔐 Tentando login via Email/Senha (Fallback)...")
        try:
            payload = {"payload": {"email": email, "password": senha, "serviceId": 438}}
            h_login = {'Content-Type': 'application/json', 'User-Agent': 'Mozilla/5.0'}
            res = requests.post("https://login.globo.com/api/authentication", json=payload, headers=h_login, timeout=TIMEOUT)
            res.raise_for_status()
            print("✅ Login realizado! (Nota: Isso é raro em servidores cloud)")
            return None # Retornaria token aqui se a lógica de login fosse completa, mas o cookie é preferível.
        except:
            print("❌ Login User/Senha falhou (Normal devido a proteção anti-bot).")
    
    return None

# --- 2. INFRAESTRUTURA ---
def get_bq_client():
    if not GCP_JSON: raise ValueError("GCP_SERVICE_ACCOUNT ausente.")
    # Tenta ler como JSON string ou Dict direto
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
    # nosec: Safe format
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
        # Usa get_headers() que agora injeta o Cookie se necessário
        res = requests.get(url, headers=get_headers(), timeout=TIMEOUT)
        if res.status_code == 200: return res.json()
        
        # Fallback público (se der 401/403)
        if res.status_code in [401, 403]:
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT)
            return res.json() if res.status_code == 200 else {'pontos': 0, 'atletas': []}
            
        return {'pontos': 0, 'atletas': []}
    except: return {'pontos': 0, 'atletas': []}

# --- 4. EXECUÇÃO PRINCIPAL ---
def rodar_coleta():
    global BEARER_TOKEN
    
    # 1. Tenta autenticação (mas não morre se falhar)
    BEARER_TOKEN = buscar_token_automatico()
    
    # Validação de segurança: Temos alguma credencial?
    if not BEARER_TOKEN and not GLBID_SECRET:
        print("⚠️ AVISO: Sem Token Bearer e sem Cookie GLBID. Acesso a ligas privadas falhará.")
    
    client = get_bq_client()
    garantir_dataset(client)

    # 2. Status Mercado
    try:
        status_api = requests.get("https://api.cartola.globo.com/mercado/status", headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT).json()
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

    # 3. Metadados
    try:
        res_clubes = requests.get("https://api.cartola.globo.com/clubes", headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT).json()
        clubes = {str(id): t['nome'] for id, t in res_clubes.items()}
        posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}
    except: clubes, posicoes = {}, {}

    # 4. BUSCA DA LIGA
    print(f"🌍 Tentando acessar dados da liga: {LIGA_SLUG}")
    
    # Tenta direto a API Privada usando a autenticação melhorada
    # (A URL /auth/liga é a mais segura para garantir que pegamos dados completos)
    url_liga = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    res_liga = requests.get(url_liga, headers=get_headers(), timeout=TIMEOUT)
    
    # Se falhar, tenta a pública como fallback
    if res_liga.status_code != 200:
        print(f"⚠️ Acesso autenticado retornou {res_liga.status_code}. Tentando endpoint público...")
        url_liga = f"https://api.cartola.globo.com/liga/{LIGA_SLUG}"
        res_liga = requests.get(url_liga, headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT)

    if res_liga.status_code != 200:
        print(f"❌ Erro fatal: Não foi possível acessar a liga. Status: {res_liga.status_code}")
        # Dica de debug
        if res_liga.status_code == 404: print("👉 Verifique se o SLUG da liga está correto.")
        return

    times_liga = res_liga.json().get('times', [])
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_hist, l_esc = [], []

    print(f"🚀 Processando {len(times_liga)} times...")

    for time_obj in times_liga:
        nome_time = time_obj['nome']
        dados_time = get_dados_time_smart(time_obj['time_id'], rodada_alvo, is_live)
        
        # Lógica de Pontos
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

    # Carga no Banco
    if l_hist:
        limpar_dados_rodada(client, rodada_alvo)
        s_hist = [bigquery.SchemaField("nome", "STRING"), bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("timestamp", "TIMESTAMP"), bigquery.SchemaField("tipo_dado", "STRING")]
        salvar_bigquery(client, pd.DataFrame(l_hist), TAB_HISTORICO, s_hist)
        
        s_esc = [bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("liga_time_nome", "STRING"), bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("is_capitao", "BOOLEAN"), bigquery.SchemaField("timestamp", "TIMESTAMP")]
        salvar_bigquery(client, pd.DataFrame(l_esc), TAB_ESCALACOES, s_esc)

    print("🏁 Coleta finalizada com sucesso.")

if __name__ == "__main__":
    rodar_coleta()
