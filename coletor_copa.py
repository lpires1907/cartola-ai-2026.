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
    info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT')) if os.getenv('GCP_SERVICE_ACCOUNT') else None
    if info:
        return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(info), project=info['project_id'])
    return bigquery.Client()

def get_token():
    return os.getenv("CARTOLA_GLBID")

def carregar_configuracao():
    if not os.path.exists(ARQUIVO_CONFIG): return []
    with open(ARQUIVO_CONFIG, 'r', encoding='utf-8') as f:
        return json.load(f)

def limpar_dados_da_copa(client, slug):
    client.query(f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{slug}'").result() # nosec B608

def caçar_jogos_recursivo(dados):
    jogos = []
    if isinstance(dados, dict):
        if 'time_mandante_id' in dados: return [dados]
        for v in dados.values(): jogos.extend(caçar_jogos_recursivo(v))
    elif isinstance(dados, list):
        for item in dados: jogos.extend(caçar_jogos_recursivo(item))
    return jogos

def buscar_parciais_globais(headers):
    try:
        res = requests.get("https://api.cartola.globo.com/atletas/pontuados", headers=headers, timeout=30).json()
        return {int(id_str): info.get('pontuacao', 0.0) for id_str, info in res.get('atletas', {}).items()}
    except: return {}

def buscar_status_partidas(headers):
    try:
        res = requests.get("https://api.cartola.globo.com/partidas", headers=headers, timeout=30).json()
        m = {}
        for p in res.get('partidas', []):
            s = p.get('status_transmissao_tr', 'DESCONHECIDO')
            m[p['clube_casa_id']] = s
            m[p['clube_visitante_id']] = s
        return m
    except: return {}

def calcular_pontuacao_completa(time_id, mapa_pontos, mapa_status, headers):
    if not time_id or str(time_id) == "0": return 0.0
    try:
        d = requests.get(f"https://api.cartola.globo.com/time/id/{time_id}", headers=headers, timeout=30).json()
        tits = []
        for t in d.get('atletas', []):
            pid = t['atleta_id']
            tits.append({'id': pid, 'pos': t['posicao_id'], 'club': t['clube_id'], 'pts': mapa_pontos.get(pid, 0.0), 'cap': (pid == d.get('capitao_id'))})
        
        res_raw = d.get('reservas', [])
        for i, t in enumerate(tits):
            if mapa_status.get(t['club']) == "ENCERRADA" and t['pts'] == 0.0:
                r = next((x for x in res_raw if x['posicao_id'] == t['pos'] and mapa_pontos.get(x['atleta_id'], 0.0) != 0.0), None)
                if r:
                    tits[i].update({'id': r['atleta_id'], 'pts': mapa_pontos.get(r['atleta_id'], 0.0)})
                    res_raw.remove(r)

        lux_id = d.get('reserva_luxo_id')
        if lux_id:
            lx = next((r for r in res_raw if r['atleta_id'] == lux_id), None)
            if lx:
                concs = [t for t in tits if t['pos'] == lx['posicao_id']]
                if concs and all(mapa_status.get(t['club']) == "ENCERRADA" for t in concs):
                    pior = min(concs, key=lambda x: x['pts'])
                    if mapa_pontos.get(lux_id, 0.0) > pior['pts']:
                        tits[tits.index(pior)].update({'pts': mapa_pontos.get(lux_id, 0.0)})

        return round(sum((t['pts'] * 1.5 if t['cap'] else t['pts']) for t in tits), 2)
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
        except: pass

if __name__ == "__main__":
    coletar_dados_copa()
