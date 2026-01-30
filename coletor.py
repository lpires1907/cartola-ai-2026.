import os
import json
import requests
import pandas as pd
import time
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

# --- CONEXÃO BIGQUERY ---
def get_bq_client():
    if not GCP_JSON: raise ValueError("GCP_SERVICE_ACCOUNT ausente.")
    info = json.loads(GCP_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

def garantir_dataset(client):
    try:
        client.create_dataset(bigquery.Dataset(f"{client.project}.{DATASET_ID}"), exists_ok=True)
    except: pass

def get_ultima_rodada_oficial_banco(client):
    """Verifica qual a última rodada OFICIAL carregada para não duplicar"""
    query = f"""
        SELECT MAX(rodada) as max_rodada 
        FROM `{client.project}.{TAB_HISTORICO}` 
        WHERE tipo_dado = 'OFICIAL'
    """
    try:
        query_job = client.query(query)
        results = list(query_job.result())
        max_rodada = results[0].max_rodada
        return max_rodada if max_rodada is not None else -1
    except:
        return -1

def salvar_bigquery(client, df, tabela, schema):
    if df.empty: return
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=schema
    )
    try:
        job = client.load_table_from_dataframe(df, f"{client.project}.{tabela}", job_config=job_config)
        job.result()
        print(f"✅ Salvo: {len(df)} linhas em {tabela}")
    except Exception as e:
        print(f"❌ Erro BQ {tabela}: {e}")

# --- API CARTOLA ---
def get_headers():
    return {
        'Authorization': f'Bearer {BEARER_TOKEN}',
        'x-glb-auth': 'oidc',
        'x-glb-app': 'cartola_web',
        'User-Agent': 'Mozilla/5.0'
    }

def get_mercado_status():
    """
    Retorna Status (1=Aberto, 2=Fechado) e a Rodada do Cartola.
    Atenção: Se mercado aberto, a 'rodada_atual' é a PRÓXIMA (que ainda não aconteceu).
    """
    try:
        res = requests.get("https://api.cartola.globo.com/mercado/status", headers=get_headers())
        return res.json()
    except: return {'status_mercado': 1, 'rodada_atual': 0}

def get_atletas_pontuados(rodada, is_live):
    url = "https://api.cartola.globo.com/atletas/pontuados" if is_live else f"https://api.cartola.globo.com/atletas/pontuados/{rodada}"
    try:
        res = requests.get(url, headers=get_headers())
        if res.status_code == 200: return res.json()
    except: pass
    return {}

def get_escalacao(time_id, rodada, is_live):
    url = f"https://api.cartola.globo.com/time/id/{time_id}" if is_live else f"https://api.cartola.globo.com/time/id/{time_id}/{rodada}"
    try:
        res = requests.get(url, headers=get_headers())
        return res.json().get('atletas', []) if res.status_code == 200 else []
    except: return []

# --- FLUXO DE CARGA ---
def main():
    if not BEARER_TOKEN: print("⛔ Token ausente."); return
    
    client = get_bq_client()
    garantir_dataset(client)

    # 1. Inteligência de Carga
    status_api = get_mercado_status()
    mercado_status = status_api.get('status_mercado', 1) # 1=Aberto, 2=Fechado
    rodada_cartola = status_api.get('rodada_atual', 0)
    
    # Define qual rodada vamos buscar
    is_live = False
    tipo_dado = "OFICIAL"
    rodada_alvo = 0

    if mercado_status == 2: 
        # Mercado Fechado = Jogo Rolando = Parciais
        is_live = True
        tipo_dado = "PARCIAL"
        rodada_alvo = rodada_cartola
        print(f"⚡ Modo LIVE: Mercado fechado. Buscando parciais da Rodada {rodada_alvo}.")
        
    else:
        # Mercado Aberto = Rodada anterior fechou = Oficial
        # Se Cartola diz rodada 3, quer dizer que a 2 acabou. Queremos a 2.
        rodada_alvo = rodada_cartola - 1
        print(f"🔒 Modo FECHADO: Mercado aberto. Verificando dados oficiais da Rodada {rodada_alvo}.")

    if rodada_alvo < 1:
        print("⏸️ Pré-temporada ou Rodada 0. Nada a fazer.")
        return

    # 2. Check de Idempotência (Já carreguei essa oficial?)
    if not is_live:
        ultima_bq = get_ultima_rodada_oficial_banco(client)
        if rodada_alvo <= ultima_bq:
            print(f"zzz Dados da Rodada {rodada_alvo} já existem no banco (Última DB: {ultima_bq}). Nenhuma ação necessária.")
            return
        else:
            print(f"🚀 Nova rodada fechada detectada! (API: {rodada_alvo} > DB: {ultima_bq}). Iniciando carga...")

    # 3. Extração e Cruzamento
    dict_atletas_pts = get_atletas_pontuados(rodada_alvo, is_live)
    
    # Busca cache de metadados
    try:
        clubes = {str(id): t['nome'] for id, t in requests.get("https://api.cartola.globo.com/clubes", headers=get_headers()).json().items()}
        posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}
    except: clubes, posicoes = {}, {}

    times_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=get_headers()).json().get('times', [])
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))

    lista_historico = []
    lista_escalacoes = []
    lista_atletas_globais = []

    # Processa Atletas Globais (se houver dados)
    if dict_atletas_pts:
        for id_atl, dados in dict_atletas_pts.items():
            lista_atletas_globais.append({
                'rodada': int(rodada_alvo),
                'atleta_id': int(id_atl),
                'atleta_apelido': str(dados.get('apelido', '')),
                'atleta_clube': clubes.get(str(dados.get('clube_id')), 'Outros'),
                'pontos': float(dados.get('pontuacao', 0.0)),
                'status_rodada': tipo_dado,
                'timestamp': ts_agora
            })

    # Processa Times da Liga
    print(f"🔄 Processando {len(times_liga)} times da liga...")
    for time_obj in times_liga:
        escalacao = get_escalacao(time_obj['time_id'], rodada_alvo, is_live)
        pontos_total_calculado = 0.0
        
        for atl in escalacao:
            pid = str(atl['atleta_id'])
            pts = float(dict_atletas_pts.get(pid, {}).get('pontuacao', 0.0))
            pontos_total_calculado += pts
            
            lista_escalacoes.append({
                'rodada': int(rodada_alvo),
                'liga_time_nome': str(time_obj['nome']),
                'atleta_apelido': str(atl.get('apelido', '')),
                'atleta_posicao': posicoes.get(str(atl.get('posicao_id')), ''),
                'pontos': pts,
                'status_rodada': tipo_dado,
                'timestamp': ts_agora
            })

        # Adiciona ao histórico (Agora com PATRIMONIO garantido)
        # Se for LIVE, usamos o calculado. Se for OFICIAL, confiamos no calculado para consistência.
        lista_historico.append({
            'nome': str(time_obj['nome']),
            'nome_cartola': str(time_obj.get('nome_cartola', '')),
            'pontos': pontos_total_calculado,
            'patrimonio': float(time_obj.get('patrimonio', 100)), # <--- NOVO CAMPO SOLICITADO
            'rodada': int(rodada_alvo),
            'timestamp': ts_agora,
            'tipo_dado': tipo_dado
        })

    # 4. Carga no BigQuery
    # Salvar Atletas Globais
    if lista_atletas_globais:
        schema = [
            bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("atleta_id", "INTEGER"),
            bigquery.SchemaField("atleta_apelido", "STRING"), bigquery.SchemaField("atleta_clube", "STRING"),
            bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("status_rodada", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]
        salvar_bigquery(client, pd.DataFrame(lista_atletas_globais), TAB_ATLETAS, schema)

    # Salvar Escalações
    if lista_escalacoes:
        schema = [
            bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("liga_time_nome", "STRING"),
            bigquery.SchemaField("atleta_apelido", "STRING"), bigquery.SchemaField("atleta_posicao", "STRING"),
            bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("status_rodada", "STRING"),
            bigquery.SchemaField("timestamp", "TIMESTAMP")
        ]
        salvar_bigquery(client, pd.DataFrame(lista_escalacoes), TAB_ESCALACOES, schema)

    # Salvar Histórico da Liga
    if lista_historico:
        schema = [
            bigquery.SchemaField("nome", "STRING"), bigquery.SchemaField("nome_cartola", "STRING"),
            bigquery.SchemaField("pontos", "FLOAT"), bigquery.SchemaField("patrimonio", "FLOAT"),
            bigquery.SchemaField("rodada", "INTEGER"), bigquery.SchemaField("timestamp", "TIMESTAMP"),
            bigquery.SchemaField("tipo_dado", "STRING")
        ]
        salvar_bigquery(client, pd.DataFrame(lista_historico), TAB_HISTORICO, schema)

    print("🏁 Processo de carga finalizado.")

if __name__ == "__main__":
    main()