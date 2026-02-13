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
ARQUIVO_CONFIG = "copas.json"
DATASET_ID = "cartola_analytics"
TAB_COPA = f"{DATASET_ID}.copa_mata_mata"

MAPA_FASES = {
    "1": "32-avos de Final", "2": "16-avos de Final", "O": "Oitavas de Final",
    "Q": "Quartas de Final", "S": "Semifinal", "F": "Final", "T": "Disputa de 3º Lugar"
}

def get_bq_client():
    if os.path.exists("credentials.json"):
        return bigquery.Client.from_service_account_json("credentials.json")
    if os.getenv('GCP_SERVICE_ACCOUNT'):
        try:
            info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
            creds = service_account.Credentials.from_service_account_info(info)
            return bigquery.Client(credentials=creds, project=info['project_id'])
        except: pass
    return bigquery.Client()

def get_token():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def carregar_configuracao():
    if not os.path.exists(ARQUIVO_CONFIG): return []
    try:
        with open(ARQUIVO_CONFIG, 'r', encoding='utf-8') as f:
            return json.load(f)
    except: return []

def limpar_dados_da_copa(client, slug):
    try:
        query = f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{slug}'" # nosec B608
        client.query(query).result()
        print(f"🧹 Dados removidos para '{slug}'.")
    except: pass

def caçar_jogos_recursivo(dados):
    jogos = []
    if isinstance(dados, dict):
        if 'time_mandante_id' in dados: return [dados]
        for v in dados.values(): jogos.extend(caçar_jogos_recursivo(v))
    elif isinstance(dados, list):
        for item in dados: jogos.extend(caçar_jogos_recursivo(item))
    return jogos

def buscar_parciais_globais(headers):
    url = "https://api.cartola.globo.com/atletas/pontuados"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            atletas = resp.json().get('atletas', {})
            return {int(id_str): info.get('pontuacao', 0.0) for id_str, info in atletas.items()}
    except: pass
    return {}

def buscar_status_partidas(headers):
    url = "https://api.cartola.globo.com/partidas"
    mapa = {}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            for p in resp.json().get('partidas', []):
                s = p.get('status_transmissao_tr', 'DESCONHECIDO')
                mapa[p['clube_casa_id']] = s
                mapa[p['clube_visitante_id']] = s
    except: pass
    return mapa

def calcular_pontuacao_completa(time_id, mapa_pontos, mapa_status, headers):
    if not time_id or str(time_id) == "0": return 0.0
    try:
        resp = requests.get(f"https://api.cartola.globo.com/time/id/{time_id}", headers=headers, timeout=30)
        if resp.status_code != 200: return 0.0
        d = resp.json()
        titulares = []
        for t in d.get('atletas', []):
            pid = t['atleta_id']
            titulares.append({'id': pid, 'pos': t['posicao_id'], 'club': t['clube_id'], 'pts': mapa_pontos.get(pid, 0.0), 'cap': (pid == d.get('capitao_id'))})
        
        reservas = d.get('reservas', [])
        for i, t in enumerate(titulares):
            if mapa_status.get(t['club']) == "ENCERRADA" and t['pts'] == 0.0:
                res = next((r for r in reservas if r['posicao_id'] == t['pos'] and mapa_pontos.get(r['atleta_id'], 0.0) != 0.0), None)
                if res:
                    titulares[i].update({'id': res['atleta_id'], 'pts': mapa_pontos.get(res['atleta_id'], 0.0), 'club': res['clube_id']})
                    reservas.remove(res)

        luxo_id = d.get('reserva_luxo_id')
        if luxo_id:
            luxo_obj = next((r for r in reservas if r['atleta_id'] == luxo_id), None)
            if luxo_obj:
                concorrentes = [t for t in titulares if t['pos'] == luxo_obj['posicao_id']]
                if concorrentes and all(mapa_status.get(t['club']) == "ENCERRADA" for t in concorrentes):
                    pior = min(concorrentes, key=lambda x: x['pts'])
                    l_pts = mapa_pontos.get(luxo_id, 0.0)
                    if l_pts > pior['pts']:
                        idx = titulares.index(pior)
                        titulares[idx].update({'id': luxo_id, 'pts': l_pts})

        return round(sum((t['pts'] * 1.5 if t['cap'] else t['pts']) for t in titulares), 2)
    except: return 0.0

def coletar_dados_copa():
    copas = carregar_configuracao()
    token = get_token()
    headers = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0'}
    client = get_bq_client()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    m_pts = buscar_parciais_globais(headers)
    m_sts = buscar_status_partidas(headers)

    for copa in copas:
        if not copa.get('ativa'): continue
        limpar_dados_da_copa(client, copa['slug'])
        try:
            res = requests.get(f"https://api.cartola.globo.com/auth/liga/{copa['slug']}", headers=headers, timeout=30).json()
            r_atual = res['liga'].get('rodada_atual', 0)
            raw_t = res.get('times', [])
            dic_t = {str(t.get('time_id') or t.get('id')): t for t in raw_t} if isinstance(raw_t, list) else raw_t
            
            jogos = caçar_jogos_recursivo(res.get('chaves_mata_mata', {}))
            l_final = []
            for j in jogos:
                r_jogo = j.get('rodada_id')
                if r_jogo == r_atual and m_pts:
                    pts_a = calcular_pontuacao_completa(str(j.get('time_mandante_id')), m_pts, m_sts, headers)
                    pts_b = calcular_pontuacao_completa(str(j.get('time_visitante_id')), m_pts, m_sts, headers)
                else:
                    pts_a = float(j.get('time_mandante_pontuacao') or 0.0)
                    pts_b = float(j.get('time_visitante_pontuacao') or 0.0)

                t_a, t_b = dic_t.get(str(j.get('time_mandante_id')), {}), dic_t.get(str(j.get('time_visitante_id')), {})
                l_final.append({
                    'nome_copa': copa['nome_visual'], 'liga_slug': copa['slug'], 'rodada_real': r_jogo,
                    'fase_copa': MAPA_FASES.get(j.get('tipo_fase'), 'Fase'),
                    'time_a_nome': t_a.get('nome', 'A Definir'), 'time_a_slug': t_a.get('slug', ''), 'time_a_pontos': pts_a,
                    'time_b_nome': t_b.get('nome', 'A Definir'), 'time_b_slug': t_b.get('slug', ''), 'time_b_pontos': pts_b,
                    'vencedor': dic_t.get(str(j.get('vencedor_id')), {}).get('slug'), 'data_coleta': ts
                })
            if l_final:
                client.load_table_from_dataframe(pd.DataFrame(l_final), TAB_COPA).result()
                print(f"✅ Liga {copa['slug']} atualizada.")
        except Exception as e: print(f"❌ Erro: {e}")

if __name__ == "__main__":
    coletar_dados_copa()
