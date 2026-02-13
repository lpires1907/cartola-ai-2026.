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

# Mapa de Fases
MAPA_FASES = {
    "1": "32-avos de Final",
    "2": "16-avos de Final",
    "O": "Oitavas de Final",
    "Q": "Quartas de Final",
    "S": "Semifinal",
    "F": "Final"
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
        # nosec: slug vem de config interna
        query = f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{slug}'" # nosec
        client.query(query).result()
        print(f"🧹 Dados removidos para '{slug}'.")
    except: pass

# --- FUNÇÕES AUXILIARES ---

def caçar_jogos_recursivo(dados):
    """
    Encontra objetos de jogo (com 'time_mandante_id') em qualquer nível de aninhamento.
    """
    jogos = []
    if isinstance(dados, dict):
        if 'time_mandante_id' in dados:
            return [dados]
        for v in dados.values():
            jogos.extend(caçar_jogos_recursivo(v))
    elif isinstance(dados, list):
        for item in dados:
            jogos.extend(caçar_jogos_recursivo(item))
    return jogos

def buscar_parciais_globais(headers):
    url = "https://api.cartola.globo.com/atletas/pontuados"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            dados = resp.json()
            mapa = {}
            for id_str, info in dados.get('atletas', {}).items():
                mapa[int(id_str)] = info.get('pontuacao', 0.0)
            print(f"      📡 Parciais: {len(mapa)} atletas pontuados.")
            return mapa
    except: pass
    return {}

def buscar_status_partidas(headers):
    url = "https://api.cartola.globo.com/partidas"
    mapa_status = {}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            partidas = resp.json().get('partidas', [])
            for p in partidas:
                status = p.get('status_transmissao_tr', 'DESCONHECIDO')
                mapa_status[p['clube_casa_id']] = status
                mapa_status[p['clube_visitante_id']] = status
    except: pass
    return mapa_status

def calcular_pontuacao_completa(time_id, mapa_pontos, mapa_status_jogos, headers):
    """
    Calcula pontuação com:
    1. Substituição Padrão (Tapa-Buraco)
    2. Substituição de Luxo (Desempenho) - Regra 2026
    3. Capitão
    """
    if not time_id or str(time_id) == "0": return 0.0

    url = f"https://api.cartola.globo.com/time/id/{time_id}"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200: return 0.0
        
        dados = resp.json()
        titulares_raw = dados.get('atletas', [])
        reservas_raw = dados.get('reservas', [])
        capitao_id = dados.get('capitao_id')
        reserva_luxo_id = dados.get('reserva_luxo_id') # Campo chave de 2026
        
        # Prepara titulares com pontuação atual
        titulares_ativos = []
        for t in titulares_raw:
            pid = t['atleta_id']
            titulares_ativos.append({
                'atleta_id': pid,
                'posicao_id': t['posicao_id'],
                'clube_id': t['clube_id'],
                'pontos': mapa_pontos.get(pid, 0.0),
                'is_capitao': (pid == capitao_id),
                'apelido': t['apelido']
            })

        # 1. SUBSTITUIÇÃO PADRÃO (Quem não jogou)
        for i, titular in enumerate(titulares_ativos):
            status_jogo = mapa_status_jogos.get(titular['clube_id'], 'PRE_JOGO')
            
            # Só substitui se jogo acabou e pontos = 0
            if status_jogo == "ENCERRADA" and titular['pontos'] == 0.0:
                # Busca reserva da mesma posição
                reserva = next((r for r in reservas_raw 
                                if r['posicao_id'] == titular['posicao_id'] 
                                and mapa_pontos.get(r['atleta_id'], 0.0) != 0.0), None)
                
                if reserva:
                    # Aplica a troca
                    titulares_ativos[i].update({
                        'atleta_id': reserva['atleta_id'],
                        'pontos': mapa_pontos.get(reserva['atleta_id'], 0.0),
                        'clube_id': reserva['clube_id'],
                        'apelido': reserva['apelido']
                    })
                    reservas_raw.remove(reserva) # Gasta o reserva

        # 2. SUBSTITUIÇÃO DE LUXO (Desempenho)
        if reserva_luxo_id:
            # Acha o objeto do Luxo se ele ainda estiver no banco
            luxo_obj = next((r for r in reservas_raw if r['atleta_id'] == reserva_luxo_id), None)
            
            if luxo_obj:
                luxo_pts = mapa_pontos.get(reserva_luxo_id, 0.0)
                pos_luxo = luxo_obj['posicao_id']
                
                # Filtra titulares da mesma posição
                concorrentes = [t for t in titulares_ativos if t['posicao_id'] == pos_luxo]
                
                if concorrentes:
                    # Verifica se TODOS os jogos da posição acabaram
                    ids_clubes = [t['clube_id'] for t in concorrentes] + [luxo_obj['clube_id']]
                    todos_fim = all(mapa_status_jogos.get(cid) == "ENCERRADA" for cid in ids_clubes)
                    
                    if todos_fim:
                        pior_titular = min(concorrentes, key=lambda x: x['pontos'])
                        
                        # Se o Luxo for melhor que o pior titular, troca!
                        if luxo_pts > pior_titular['pontos']:
                            idx = titulares_ativos.index(pior_titular)
                            titulares_ativos[idx].update({
                                'atleta_id': reserva_luxo_id,
                                'pontos': luxo_pts,
                                'clube_id': luxo_obj['clube_id']
                                # Mantém capitania se titular era capitão (herança padrão)
                            })

        # 3. SOMA FINAL
        soma = 0.0
        for t in titulares_ativos:
            pts = t['pontos']
            if t['is_capitao']: soma += (pts * 1.5)
            else: soma += pts
            
        time.sleep(0.1)
        return round(soma, 2)

    except: return 0.0

# --- FLUXO PRINCIPAL ---

def coletar_dados_copa():
    copas = carregar_configuracao()
    if not copas: return
    token = get_token()
    if not token: 
        print("❌ Token não encontrado.")
        return
    
    headers = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0'}
    client = get_bq_client()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    
    # Prepara dados globais
    mapa_parciais = buscar_parciais_globais(headers)
    mapa_status = buscar_status_partidas(headers)
    
    print(f"🏆 Processando Copas...")

    for copa in copas:
        slug = copa.get('slug')
        if not copa.get('ativa'): continue
        
        print(f"   🔄 Liga: {slug}")
        limpar_dados_da_copa(client, slug)

        try:
            resp = requests.get(f"https://api.cartola.globo.com/auth/liga/{slug}", headers=headers, timeout=30)
            if resp.status_code != 200:
                print(f"      ❌ Erro API: {resp.status_code}")
                continue

            dados = resp.json()
            
            # --- CAÇADOR DE JOGOS ---
            raw_chaves = dados.get('chaves_mata_mata', {})
            todos_jogos_brutos = caçar_jogos_recursivo(raw_chaves)
            
            print(f"      🔎 Jogos encontrados: {len(todos_jogos_brutos)}")
            
            dic_times = dados.get('times', {})
            lista_final = []
            rodada_atual = dados['liga'].get('rodada_atual', 0)

            for jogo in todos_jogos_brutos:
                try:
                    # --- CORREÇÃO DE LISTA (O GRANDE FIX) ---
                    # Se por acaso o 'jogo' ainda estiver dentro de uma lista (ex: [dict]), desenbrulha
                    if isinstance(jogo, list):
                        if len(jogo) > 0: jogo = jogo[0]
                        else: continue
                    
                    if not isinstance(jogo, dict):
                        continue

                    # Extração segura
                    id_a = str(jogo.get('time_mandante_id'))
                    id_b = str(jogo.get('time_visitante_id'))
                    id_win = str(jogo.get('vencedor_id'))
                    
                    # Definição de Pontos (API ou Cálculo)
                    pts_a_api = float(jogo.get('time_mandante_pontuacao') or 0.0)
                    pts_b_api = float(jogo.get('time_visitante_pontuacao') or 0.0)
                    
                    pts_a = pts_a_api
                    pts_b = pts_b_api
                    
                    # Se tiver parciais, força o cálculo para aplicar Reserva de Luxo
                    if mapa_parciais:
                        pts_a = calcular_pontuacao_completa(id_a, mapa_parciais, mapa_status, headers)
                        pts_b = calcular_pontuacao_completa(id_b, mapa_parciais, mapa_status, headers)
                        
                        # Fallback se der erro no cálculo e API tiver valor
                        if pts_a == 0.0 and pts_a_api > 0: pts_a = pts_a_api
                        if pts_b == 0.0 and pts_b_api > 0: pts_b = pts_b_api

                    # Montagem do registro
                    t_a = dic_times.get(id_a, {})
                    t_b = dic_times.get(id_b, {})
                    
                    fase = MAPA_FASES.get(jogo.get('tipo_fase'), 'Fase')
                    
                    win_slug = None
                    if id_win == id_a: win_slug = t_a.get('slug')
                    elif id_win == id_b: win_slug = t_b.get('slug')

                    lista_final.append({
                        'nome_copa': copa.get('nome_visual'),
                        'liga_slug': slug,
                        'rodada_real': rodada_atual,
                        'fase_copa': fase,
                        'time_a_nome': t_a.get('nome', f'Time {id_a}'),
                        'time_a_slug': str(t_a.get('slug', id_a)),
                        'time_a_escudo': t_a.get('url_escudo_png', ''),
                        'time_a_pontos': pts_a,
                        'time_b_nome': t_b.get('nome', f'Time {id_b}'),
                        'time_b_slug': str(t_b.get('slug', id_b)),
                        'time_b_escudo': t_b.get('url_escudo_png', ''),
                        'time_b_pontos': pts_b,
                        'vencedor': win_slug,
                        'data_coleta': ts
                    })

                except Exception as e:
                    print(f"      ⚠️ Erro processando jogo: {e}")

            if lista_final:
                df = pd.DataFrame(lista_final)
                job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("nome_copa", "STRING"),
                        bigquery.SchemaField("liga_slug", "STRING"),
                        bigquery.SchemaField("rodada_real", "INTEGER"),
                        bigquery.SchemaField("fase_copa", "STRING"),
                        bigquery.SchemaField("time_a_nome", "STRING"),
                        bigquery.SchemaField("time_a_slug", "STRING"),
                        bigquery.SchemaField("time_a_escudo", "STRING"),
                        bigquery.SchemaField("time_a_pontos", "FLOAT"),
                        bigquery.SchemaField("time_b_nome", "STRING"),
                        bigquery.SchemaField("time_b_slug", "STRING"),
                        bigquery.SchemaField("time_b_escudo", "STRING"),
                        bigquery.SchemaField("time_b_pontos", "FLOAT"),
                        bigquery.SchemaField("vencedor", "STRING"),
                        bigquery.SchemaField("data_coleta", "TIMESTAMP"),
                    ],
                    write_disposition="WRITE_APPEND",
                    schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
                )
                client.load_table_from_dataframe(df, TAB_COPA, job_config=job_config).result()
                print(f"      ✅ SUCESSO! {len(df)} jogos salvos (Com Luxo e Parciais).")
            else:
                print("      ⚠️ Nenhum jogo processado.")
                
        except Exception as e:
            print(f"      ❌ Erro fatal: {e}")

if __name__ == "__main__":
    coletar_dados_copa()
