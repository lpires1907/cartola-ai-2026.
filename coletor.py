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

def get_pro_headers():
    if not TOKEN_SECRET: return None
    t = TOKEN_SECRET.replace("Bearer ", "").strip().strip('"').strip("'")
    return {'authority': 'api.cartola.globo.com', 'authorization': f'Bearer {t}', 'x-glb-app': 'cartola_web', 'x-glb-auth': 'oidc', 'user-agent': 'Mozilla/5.0'}

def get_public_headers():
    return {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}

def get_bq_client():
    info = json.loads(GCP_JSON) if isinstance(GCP_JSON, str) else GCP_JSON
    return bigquery.Client(credentials=service_account.Credentials.from_service_account_info(info), project=info['project_id'])

def limpar_dados_rodada(client, rodada):
    print(f"🧹 Limpando todos os registros (Parciais e Oficiais) da Rodada {rodada}...")
    for t in [TAB_HISTORICO, TAB_ESCALACOES]:
        # Deleta qualquer dado da rodada para evitar somas duplicadas
        client.query(f"DELETE FROM `{client.project}.{t}` WHERE rodada = {rodada}").result() # nosec B608

def buscar_parciais_globais():
    try:
        res = requests.get("https://api.cartola.globo.com/atletas/pontuados", headers=get_public_headers(), timeout=TIMEOUT).json()
        return {int(id_str): info.get('pontuacao', 0.0) for id_str, info in res.get('atletas', {}).items()}
    except: return {}

def buscar_status_partidas():
    try:
        res = requests.get("https://api.cartola.globo.com/partidas", headers=get_public_headers(), timeout=TIMEOUT).json()
        m = {}
        for p in res.get('partidas', []):
            s = p.get('status_transmissao_tr', 'PRE_JOGO')
            m[p['clube_casa_id']] = s
            m[p['clube_visitante_id']] = s
        return m
    except: return {}

def calcular_pontos(dados, m_pts, m_sts):
    tits = []
    for t in dados.get('atletas', []):
        aid = t['atleta_id']
        tits.append({'id': aid, 'pos': t['posicao_id'], 'club': t['clube_id'], 'ap': t['apelido'], 'pts': m_pts.get(aid, 0.0), 'cap': (aid == dados.get('capitao_id'))})
    
    res_raw = dados.get('reservas', [])
    for i, t in enumerate(tits):
        if m_sts.get(t['club']) == "ENCERRADA" and t['pts'] == 0.0:
            r = next((x for x in res_raw if x['posicao_id'] == t['pos'] and m_pts.get(x['atleta_id'], 0.0) != 0.0), None)
            if r:
                tits[i].update({'id': r['atleta_id'], 'pts': m_pts.get(r['atleta_id'], 0.0), 'ap': r['apelido']})
                res_raw.remove(r)
    
    lux_id = dados.get('reserva_luxo_id')
    if lux_id:
        lx = next((r for r in res_raw if r['atleta_id'] == lux_id), None)
        if lx:
            concs = [t for t in tits if t['pos'] == lx['posicao_id']]
            if concs and all(m_sts.get(t['club']) == "ENCERRADA" for t in concs):
                pior = min(concs, key=lambda x: x['pts'])
                if m_pts.get(lux_id, 0.0) > pior['pts']:
                    tits[tits.index(pior)].update({'id': lux_id, 'pts': m_pts.get(lux_id, 0.0), 'ap': lx['apelido']})

    return round(sum((t['pts'] * 1.5 if t['cap'] else t['pts']) for tit in tits), 2), tits

def rodar_coleta():
    client = get_bq_client()
    st = requests.get("https://api.cartola.globo.com/mercado/status", headers=get_public_headers(), timeout=TIMEOUT).json()
    r_atual = st.get('rodada_atual', 0)
    mercado_aberto = (st.get('status_mercado') == 1)
    
    # Se mercado está aberto, a rodada que acabou de fechar é a anterior
    r_alvo = (r_atual - 1) if mercado_aberto else r_atual
    is_live = (st.get('status_mercado') == 2)
    tipo_dado = "PARCIAL" if is_live else "OFICIAL"
    
    m_pts = buscar_parciais_globais() if is_live else {}
    m_sts = buscar_status_partidas() if is_live else {}

    res_liga = requests.get(f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}", headers=get_pro_headers(), timeout=TIMEOUT).json()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    l_h, l_e = [], []
    pos = {'1': 'Goleiro', '2': 'Lateral', '3': 'Zagueiro', '4': 'Meia', '5': 'Atacante', '6': 'Técnico'}

    for t_obj in res_liga.get('times', []):
        res_t = requests.get(f"https://api.cartola.globo.com/time/id/{t_obj['time_id']}", headers=get_public_headers(), timeout=TIMEOUT).json()
        
        info_t = res_t.get('time', {})
        v_nome_cartola = info_t.get('nome_cartola') or t_obj.get('nome_cartola') or "Sem Nome"
        v_patrimonio = float(res_t.get('patrimonio') or info_t.get('patrimonio') or 0.0)

        # Se for oficial, usa o ponto que vem direto do objeto da liga/time para evitar erros de cálculo manual
        if not is_live:
            pts = float(t_obj.get('pontos', {}).get('rodada', 0.0))
            atl_f = res_t.get('atletas', [])
        else:
            pts, atl_f = calcular_pontos(res_t, m_pts, m_sts)

        l_h.append({
            'nome': t_obj['nome'], 
            'nome_cartola': v_nome_cartola, 
            'pontos': float(pts), 
            'patrimonio': v_patrimonio, 
            'rodada': int(r_alvo), 
            'timestamp': ts, 
            'tipo_dado': tipo_dado
        })
        for a in atl_f:
            l_e.append({
                'rodada': int(r_alvo), 
                'liga_time_nome': t_obj['nome'], 
                'atleta_apelido': a.get('ap') or a.get('apelido'), 
                'atleta_posicao': pos.get(str(a.get('pos') or a.get('posicao_id')), ''), 
                'pontos': float(a.get('pts') or a.get('pontos_num', 0.0)), 
                'is_capitao': a.get('cap') or (a.get('atleta_id') == res_t.get('capitao_id')), 
                'timestamp': ts
            })
        time.sleep(0.1)

    if l_h:
        limpar_dados_rodada(client, r_alvo)
        client.load_table_from_dataframe(pd.DataFrame(l_h), f"{client.project}.{TAB_HISTORICO}").result()
        client.load_table_from_dataframe(pd.DataFrame(l_e), f"{client.project}.{TAB_ESCALACOES}").result()
        print(f"✅ Liga sincronizada para a Rodada {r_alvo} ({tipo_dado})!")

if __name__ == "__main__":
    rodar_coleta()
