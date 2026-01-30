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

# Credenciais e Tabelas
BEARER_TOKEN = os.getenv('CARTOLA_BEARER_TOKEN')
GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_ESCALACOES = f"{DATASET_ID}.times_escalacoes"
TAB_ATLETAS = f"{DATASET_ID}.atletas_globais"

# --- INFRAESTRUTURA ---
def get_bq_client():
    if not GCP_JSON: raise ValueError("GCP_SERVICE_ACCOUNT ausente.")
    info = json.loads(GCP_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

def garantir_dataset(client):
    try: client.create_dataset(bigquery.Dataset(f"{client.project}.{DATASET_ID}"), exists_ok=True)
    except: pass

def get_ultima_rodada_oficial_banco(client):
    """Verifica qual a última rodada OFICIAL carregada"""
    try:
        query = f"SELECT MAX(rodada) as max_rodada FROM `{client.project}.{TAB_HISTORICO}` WHERE tipo_dado = 'OFICIAL'"
        results = list(client.query(query).result())
        return results[0].max_rodada if results[0].max_rodada is not None else -1
    except: return -1

def limpar_dados_rodada(client, rodada):
    """
    Remove dados existentes da rodada para evitar duplicatas antes da nova carga.
    """
    print(f"🧹 Limpando dados antigos da Rodada {rodada} (se houver)...")
    try:
        sqls = [
            f"DELETE FROM `{client.project}.{TAB_HISTORICO}` WHERE rodada = {rodada}",
            f"DELETE FROM `{client.project}.{TAB_ESCALACOES}` WHERE rodada = {rodada}",
            f"DELETE FROM `{client.project}.{TAB_ATLETAS}` WHERE rodada = {rodada}"
        ]
        
        for sql in sqls:
            try:
                client.query(sql).result()
            except Exception as e:
                # Ignora erro se a tabela não existir ainda
                if "Not found" not in str(e):
                    print(f"⚠️ Erro ao limpar tabela: {e}")
                    
        print(f"✨ Rodada {rodada} limpa com sucesso.")
    except Exception as e:
        print(f"❌ Erro crítico na limpeza: {e}")

def salvar_bigquery(client, df, tabela, schema):
    if df.empty: return
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=schema
    )
    try:
        client.load_table_from_dataframe(df, f"{client.project}.{tabela}", job_config=job_config).result()
        print(f"✅ Salvo: {len(df)} linhas em {tabela}")
    except Exception as e:
        print(f"❌ Erro BQ {tabela}: {e}")

# --- API HELPERS ---
def get_headers():
    return {'Authorization': f'Bearer {BEARER_TOKEN}', 'x-glb-auth': 'oidc', 'x-glb-app': 'cartola_web', 'User-Agent': 'Mozilla/5.0'}

def get_mercado_status():
    try: return requests.get("https://api.cartola.globo.com/mercado/status", headers=get_headers()).json()
    except: return {'status_mercado': 1, 'rodada_atual': 0}

def get_atletas_pontuados(rodada, is_live):
    url = "https://api.cartola.globo.com/atletas/pontuados" if is_live else f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
    try:
        res = requests.get(url, headers=get_headers())
        return res.json() if res.status_code == 200 else {}
    except: return {}

def get_time_completo(time_id, rodada, is_live):
    url = f"https://api.cartola.globo.com/time/id/{time_id}" if is_live else f"https://api.cartola.globo.com/time/id/{time_id}/{rodada}"
    try:
        res = requests.get(url, headers=get_headers())
        return res.json() if res.status_code == 200 else {}
    except: return {}

# --- MAIN ---
def main():
    if not BEARER_TOKEN: print("⛔ Token ausente."); return
    client = get_bq_client()
    garantir_dataset(client)

    # 1. Inteligência de Carga
    status_api = get_mercado_status()
    mercado_status = status_api.get('status_mercado', 1) 
    rodada_cartola = status_api.get('rodada_atual', 0)
    
    is_live = (mercado_status == 2)
    tipo_dado = "PARCIAL" if is_live else "OFICIAL"
    rodada_alvo = rodada_cartola if is_live else (rodada_cartola - 1)

    print(f"🔄 Rodada Alvo: {rodada_alvo} | Status: {tipo_dado}")

    if rodada_alvo < 1: print("⏸️ Rodada 0. Nada a fazer."); return

    if not is_live:
        ultima_bq = get_ultima_rodada_oficial_banco(client)
        if rodada_alvo <= ultima_bq: 
            print(f"zzz Dados OFICIAIS da rodada {rodada_alvo} já existem. Nada a fazer."); return

    # 2. Metadados e Pontos
    raw_pts = get_atletas_pontuados(rodada_alvo, is_live)
    dict_atletas_pts = raw_pts.get('atletas', raw_pts) if isinstance(raw_pts, dict) else {}
    
    try:
        clubes = {str(id): t['nome'] for id, t in requests.get("https://api.cartola.globo.com/clubes", headers=get_headers()).json().items()}
        posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}
    except: clubes, posicoes = {}, {}

    times_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=get_headers()).json().get('times', [])
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))

    l_hist, l_esc, l_atl = [], [], []

    # Processa Atletas Globais
    for id_atl, dados in dict_atletas_pts.items():
        if not str(id_atl).isdigit(): continue
        pos_id = str(dados.get('posicao_id', ''))
        
        l_atl.append({
            'rodada': int(rodada_alvo), 'atleta_id': int(id_atl), 'atleta_apelido': str(dados.get('apelido', '')),
            'atleta_clube': clubes.get(str(dados.get('clube_id')), 'Outros'), 
            'atleta_posicao': posicoes.get(pos_id, 'Outros'),
            'pontos': float(dados.get('pontuacao', 0.0)),
            'status_rodada': tipo_dado, 'timestamp': ts_agora
        })

    # Processa Times
    print(f"🔄 Processando {len(times_liga)} times...")
    for time_obj in times_liga:
        dados_time = get_time_completo(time_obj['time_id'], rodada_alvo, is_live)
        atletas = dados_time.get('atletas', [])
        capitao_id = dados_time.get('capitao_id')
        
        pontos_total = 0.0
        
        for atl in atletas:
            pid = str(atl['atleta_id'])
            # Pega pontos do dicionário global ou 0.0
            pts = float(dict_atletas_pts[pid].get('pontuacao', 0.0)) if pid in dict_atletas_pts else 0.0
            pontos_total += pts
            
            eh_capitao = (int(pid) == int(capitao_id)) if capitao_id else False

            l_esc.append({
                'rodada': int(rodada_alvo), 'liga_time_nome': str(time_obj['nome']),
                'atleta_apelido': str(atl.get('apelido', '')), 'atleta_posicao': posicoes.get(str(atl.get('posicao_id')), ''),
                'pontos': pts, 'is_capitao': bool(eh_capitao),
                'status_rodada': tipo_dado, 'timestamp': ts_agora
            })

        l_hist.append({
            'nome': str(time_obj['nome']), 'nome_cartola': str(time_obj.get('nome_cartola', '')),
            'pontos': pontos_total, 'patrimonio': float(time_obj.get('patrimonio', 100)),
            'rodada': int(rodada_alvo), 'timestamp': ts_agora, 'tipo_dado': tipo_dado
        })

    # --- LIMPEZA PRÉ-CARGA ---
    if l_hist:
        limpar_dados_rodada(client, rodada_alvo)

    # 3. Carga
    if l_atl:
        s_atl = [bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("atleta_id", "INTEGER"),
                 bigquery.SchemaField("atleta_apelido", "STRING"), bigquery.SchemaField("atleta_clube", "STRING"),
                 bigquery.SchemaField("atleta_posicao", "STRING"),
                 bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("status_rodada", "STRING"), 
                 bigquery.SchemaField("timestamp", "TIMESTAMP")]
        salvar_bigquery(client, pd.DataFrame(l_atl), TAB_ATLETAS, s_atl)

    if l_esc:
        s_esc = [bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("liga_time_nome", "STRING"),
                 bigquery.SchemaField("atleta_apelido", "STRING"), bigquery.SchemaField("atleta_posicao", "STRING"),
                 bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("is_capitao", "BOOLEAN"),
                 bigquery.SchemaField("status_rodada", "STRING"), bigquery.SchemaField("timestamp", "TIMESTAMP")]
        salvar_bigquery(client, pd.DataFrame(l_esc), TAB_ESCALACOES, s_esc)

    if l_hist:
        s_hist = [bigquery.SchemaField("nome", "STRING"), bigquery.SchemaField("nome_cartola", "STRING"),
                  bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("patrimonio", "FLOAT"),
                  bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("timestamp", "TIMESTAMP"),
                  bigquery.SchemaField("tipo_dado", "STRING")]
        salvar_bigquery(client, pd.DataFrame(l_hist), TAB_HISTORICO, s_hist)

    print("🏁 Fim.")

if __name__ == "__main__":
    main()