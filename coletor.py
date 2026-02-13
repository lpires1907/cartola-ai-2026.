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
    print(f"🧹 Limpando dados da Rodada {rodada}...")
    sqls = [f"DELETE FROM `{client.project}.{t}` WHERE rodada = {rodada}" for t in [TAB_HISTORICO, TAB_ESCALACOES]] # nosec B608
    for sql in sqls:
        try: client.query(sql).result()
        except: pass

def salvar_bigquery(client, df, tabela):
    if df.empty: return
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION])
    client.load_table_from_dataframe(df, f"{client.project}.{tabela}", job_config=job_config).result()

# --- 3. ENGINE DE CÁLCULO (IGUAL À COPA) ---

def buscar_parciais_globais():
    url = "https://api.cartola.globo.com/atletas/pontuados"
    try:
        res = requests.get(url, headers=get_public_headers(), timeout=TIMEOUT)
        if res.status_code == 200:
            atletas = res.json().get('atletas', {})
            return {int(id_str): info.get('pontuacao', 0.0) for id_str, info in atletas.items()}
    except: pass
    return {}

def buscar_status_partidas():
    url = "https://api.cartola.globo.com/partidas"
    mapa_status = {}
    try:
        res = requests.get(url, headers=get_public_headers(), timeout=TIMEOUT)
        if res.status_code == 200:
            partidas = res.json().get('partidas', [])
            for p in partidas:
                status = p.get('status_transmissao_tr', 'PRE_JOGO')
                mapa_status[p['clube_casa_id']] = status
                mapa_status[p['clube_visitante_id']] = status
    except: pass
    return mapa_status

def calcular_pontuacao_completa(dados_time, mapa_pontos, mapa_status_jogos):
    titulares_raw = dados_time.get('atletas', [])
    reservas_raw = dados_time.get('reservas', [])
    capitao_id = dados_time.get('capitao_id')
    reserva_luxo_id = dados_time.get('reserva_luxo_id')
    
    titulares_ativos = []
    for t in titulares_raw:
        pid = t['atleta_id']
        titulares_ativos.append({
            'atleta_id': pid, 'posicao_id': t['posicao_id'], 'clube_id': t['clube_id'],
            'apelido': t['apelido'], 'pontos': mapa_pontos.get(pid, 0.0), 'is_capitao': (pid == capitao_id)
        })

    # Substituição Simples (Se não pontuou e jogo acabou)
    for i, titular in enumerate(titulares_ativos):
        if mapa_status_jogos.get(titular['clube_id']) == "ENCERRADA" and titular['pontos'] == 0.0:
            reserva = next((r for r in reservas_raw if r['posicao_id'] == titular['posicao_id'] and mapa_pontos.get(r['atleta_id'], 0.0) != 0.0), None)
            if reserva:
                titulares_ativos[i].update({'atleta_id': reserva['atleta_id'], 'pontos': mapa_pontos.get(reserva['atleta_id'], 0.0), 'apelido': reserva['apelido']})
                reservas_raw.remove(reserva)

    # Reserva de Luxo
    if reserva_luxo_id:
        luxo_obj = next((r for r in reservas_raw if r['atleta_id'] == reserva_luxo_id), None)
        if luxo_obj:
            luxo_pts = mapa_pontos.get(reserva_luxo_id, 0.0)
            concorrentes = [t for t in titulares_ativos if t['posicao_id'] == luxo_obj['posicao_id']]
            if concorrentes and all(mapa_status_jogos.get(t['clube_id']) == "ENCERRADA" for t in concorrentes):
                pior = min(concorrentes, key=lambda x: x['pontos'])
                if luxo_pts > pior['pontos']:
                    idx = titulares_ativos.index(pior)
                    titulares_ativos[idx].update({'atleta_id': reserva_luxo_id, 'pontos': luxo_pts, 'apelido': luxo_obj['apelido']})

    soma = sum((t['pontos'] * 1.5 if t['is_capitao'] else t['pontos']) for t in titulares_ativos)
    return round(soma, 2), titulares_ativos

# --- 4. EXECUÇÃO ---
def rodar_coleta():
    client = get_bq_client()
    status_api = requests.get("https://api.cartola.globo.com/mercado/status", headers=get_public_headers(), timeout=TIMEOUT).json()
    rodada_alvo = status_api.get('rodada_atual', 0)
    is_live = (status_api.get('status_mercado') == 2)
    tipo_dado = "PARCIAL" if is_live else "OFICIAL"
    
    mapa_parciais = buscar_parciais_globais() if is_live else {}
    mapa_status = buscar_status_partidas() if is_live else {}

    res_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=get_pro_headers(), timeout=TIMEOUT)
    if res_liga.status_code != 200: return

    times_liga = res_liga.json().get('times', [])
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_hist, l_esc = [], []
    posicoes = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}

    for t_obj in times_liga:
        tid = t_obj['time_id']
        url_time = f"https://api.cartola.globo.com/time/id/{tid}"
        res_t = requests.get(url_time, headers=get_public_headers(), timeout=TIMEOUT)
        
        if res_t.status_code == 200:
            dados_time = res_t.json()
            
            # Cálculo unificado (igual à copa)
            if is_live:
                pts_total, atletas_finais = calcular_pontuacao_completa(dados_time, mapa_parciais, mapa_status)
            else:
                pts_total = t_obj.get('pontos', {}).get('rodada', 0.0)
                atletas_finais = [{'apelido': a['apelido'], 'posicao_id': a['posicao_id'], 'pontos': a.get('pontos_num', 0.0), 'is_capitao': (a['atleta_id'] == dados_time.get('capitao_id'))} for a in dados_time.get('atletas', [])]

            # Correção: Captura de Nome do Cartola e Patrimônio
            l_hist.append({
                'nome': t_obj.get('nome'),
                'nome_cartola': t_obj.get('nome_cartola'), # Nome do dono do time
                'pontos': float(pts_total),
                'patrimonio': float(dados_time.get('patrimonio', 0.0)), # Patrimônio atualizado
                'rodada': int(rodada_alvo),
                'timestamp': ts_agora,
                'tipo_dado': tipo_dado
            })

            for a in atletas_finais:
                l_esc.append({
                    'rodada': int(rodada_alvo), 'liga_time_nome': t_obj.get('nome'),
                    'atleta_apelido': a['apelido'], 'atleta_posicao': posicoes.get(str(a['posicao_id']), ''),
                    'pontos': float(a['pontos']), 'is_capitao': a['is_capitao'],
                    'timestamp': ts_agora
                })
        time.sleep(0.2)

    if l_hist:
        limpar_dados_rodada(client, rodada_alvo)
        salvar_bigquery(client, pd.DataFrame(l_hist), TAB_HISTORICO)
        salvar_bigquery(client, pd.DataFrame(l_esc), TAB_ESCALACOES)
        print(f"✅ Liga sincronizada com as parciais da Copa!")

if __name__ == "__main__":
    rodar_coleta()
