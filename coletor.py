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
TIMEOUT = 30 # Timeout para evitar travamento do Bandit/Actions

# --- 1. AUTENTICAÇÃO HÍBRIDA (COOKIE OU LOGIN) ---
def buscar_token_automatico():
    # Tenta pegar primeiro o Cookie direto (Mais estável)
    cookie_glb_id = os.getenv('CARTOLA_GLBID')
    
    # Se não tiver cookie, tenta user/senha (Legado/Fallback)
    email = os.getenv('CARTOLA_EMAIL')
    senha = os.getenv('CARTOLA_SENHA')
    
    token_cartola = None

    # ESTRATÉGIA A: Usar Cookie glbId (Recomendado)
    if cookie_glb_id:
        print("🍪 Usando Cookie GLBID fornecido via Secrets...")
        try:
            headers_token = {
                'Cookie': f'glbId={cookie_glb_id}',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            res = requests.get("https://api.cartola.globo.com/auth/token", headers=headers_token, timeout=TIMEOUT)
            res.raise_for_status()
            token_cartola = res.json().get('token')
            print("✅ Token renovado via Cookie com sucesso!")
            return token_cartola
        except Exception as e:
            print(f"⚠️ Falha ao renovar via Cookie: {e}. Tentando login user/senha...")
    
    # ESTRATÉGIA B: Login com User/Senha (Se o cookie falhar ou não existir)
    if not email or not senha:
        print("⚠️ Sem credenciais de login. Tentaremos acesso público.")
        return None

    print("🔐 Tentando login via Email/Senha...")
    try:
        payload = {"payload": {"email": email, "password": senha, "serviceId": 438}}
        auth_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Origin": "https://login.globo.com",
            "Referer": "https://login.globo.com/login/438"
        }
        res = requests.post("https://login.globo.com/api/authentication", json=payload, headers=auth_headers, timeout=TIMEOUT)
        res.raise_for_status()
        novo_glb_id = res.json().get('glbId')
        
        headers_token = {'Cookie': f'glbId={novo_glb_id}', 'User-Agent': auth_headers['User-Agent']}
        res_auth = requests.get("https://api.cartola.globo.com/auth/token", headers=headers_token, timeout=TIMEOUT)
        res_auth.raise_for_status()
        
        print("✅ Token renovado via Login com sucesso!")
        return res_auth.json().get('token')
    except Exception as e:
        print(f"❌ Falha crítica no login: {e}")
        return None

# --- 2. INFRAESTRUTURA ---
def get_bq_client():
    if not GCP_JSON: raise ValueError("GCP_SERVICE_ACCOUNT ausente.")
    info = json.loads(GCP_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

def garantir_dataset(client):
    try: client.create_dataset(bigquery.Dataset(f"{client.project}.{DATASET_ID}"), exists_ok=True)
    except: pass

def limpar_dados_rodada(client, rodada):
    print(f"🧹 Limpando dados da Rodada {rodada}...")
    # nosec: BigQuery table name injection safe here
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
def get_headers():
    headers = {'User-Agent': 'Mozilla/5.0'}
    if BEARER_TOKEN:
        headers['Authorization'] = f'Bearer {BEARER_TOKEN}'
        headers['x-glb-auth'] = 'oidc'
    return headers

def get_dados_time_smart(time_id, rodada, is_live):
    # Define URL (Parcial ou Oficial)
    url = f"https://api.cartola.globo.com/time/parcial/{time_id}" if is_live else f"https://api.cartola.globo.com/time/id/{time_id}/{rodada}"
    
    try:
        # Tenta com os headers atuais (com ou sem token)
        res = requests.get(url, headers=get_headers(), timeout=TIMEOUT)
        
        # SUCESSO
        if res.status_code == 200: return res.json()
        
        # ERRO 401 (Token inválido ou expirado) -> Tenta fallback público
        if res.status_code == 401:
            # Tenta sem token (modo visitante)
            res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT)
            return res.json() if res.status_code == 200 else {'pontos': 0, 'atletas': []}
            
        return {'pontos': 0, 'atletas': []}
    except: return {'pontos': 0, 'atletas': []}

# --- 4. EXECUÇÃO PRINCIPAL ---
def rodar_coleta():
    global BEARER_TOKEN
    
    # 1. Tenta obter token (Login)
    BEARER_TOKEN = buscar_token_automatico()
    
    client = get_bq_client()
    garantir_dataset(client)

    # 2. Verifica status do mercado (Público)
    try:
        status_api = requests.get("https://api.cartola.globo.com/mercado/status", headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT).json()
    except Exception as e:
        print(f"❌ Erro fatal ao checar mercado: {e}")
        return

    mercado_status = status_api.get('status_mercado', 1) 
    rodada_cartola = status_api.get('rodada_atual', 0)
    game_over = status_api.get('game_over', False)
    
    is_live = (mercado_status == 2)
    # Regra: Se mercado fechado e jogo acabou = Previa. Se aberto = Oficial da anterior.
    tipo_dado = "PREVIA" if (is_live and game_over) else ("PARCIAL" if is_live else "OFICIAL")
    rodada_alvo = rodada_cartola if is_live else (rodada_cartola - 1)

    print(f"🔄 Rodada Alvo: {rodada_alvo} ({tipo_dado})")

    # 3. Coleta Metadados (Público)
    try:
        res_clubes = requests.get("https://api.cartola.globo.com/clubes", headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT).json()
        clubes = {str(id): t['nome'] for id, t in res_clubes.items()}
        posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}
    except: clubes, posicoes = {}, {}

    # 4. BUSCA DA LIGA (Lógica Híbrida: Público -> Autenticado)
    print(f"🌍 Tentando acessar dados da liga: {LIGA_SLUG}")
    
    # TENTATIVA 1: Endpoint Público (Não precisa de login)
    url_publica = f"https://api.cartola.globo.com/liga/{LIGA_SLUG}"
    res_liga = requests.get(url_publica, headers={'User-Agent': 'Mozilla/5.0'}, timeout=TIMEOUT)
    
    if res_liga.status_code == 200:
        print("✅ Acesso via API Pública funcionou!")
    
    # TENTATIVA 2: Endpoint Autenticado (Se o público falhar com 401/403/404)
    else:
        print(f"⚠️ API Pública retornou {res_liga.status_code}. Tentando API Autenticada...")
        
        if not BEARER_TOKEN:
            print("❌ Erro: Liga privada requer login, mas o Token não foi gerado. Verifique CARTOLA_EMAIL/SENHA.")
            return

        url_privada = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
        res_liga = requests.get(url_privada, headers=get_headers(), timeout=TIMEOUT)
        
        if res_liga.status_code != 200:
            print(f"❌ Erro fatal: Não foi possível acessar a liga nem com login. Código: {res_liga.status_code}")
            return

    times_liga = res_liga.json().get('times', [])
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_hist, l_esc = [], []

    print(f"🚀 Processando {len(times_liga)} times...")

    for time_obj in times_liga:
        nome_time = time_obj['nome']
        
        # Dados detalhados do time (Escalação)
        dados_time = get_dados_time_smart(time_obj['time_id'], rodada_alvo, is_live)
        
        # Pega pontuação total (fallback para 0.0 se falhar)
        pontos_totais = float(dados_time.get('pontos', time_obj.get('pontos', {}).get('rodada', 0.0)))
        
        l_hist.append({
            'nome': nome_time, 'nome_cartola': time_obj.get('nome_cartola', ''),
            'pontos': pontos_totais, 'patrimonio': float(time_obj.get('patrimonio', 100)),
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