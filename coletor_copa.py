import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# Importa o módulo utilitário que criamos
import cartola_utils 

# --- CONFIGURAÇÕES ---
ARQUIVO_CONFIG = "copas.json"
DATASET_ID = "cartola_analytics"
TAB_COPA = f"{DATASET_ID}.copa_mata_mata"
TAB_DIM_TIMES = f"{DATASET_ID}.dim_times"

MAPA_FASES = {
    "1": "32-avos de Final", 
    "2": "16-avos de Final", 
    "O": "Oitavas de Final",
    "Q": "Quartas de Final", 
    "S": "Semifinal", 
    "F": "Final", 
    "T": "Disputa de 3º Lugar",
    "3": "Disputa de 3º Lugar" # Adicionado por segurança para a fase de 3º lugar
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
    return os.getenv("CARTOLA_GLBID")

def carregar_configuracao():
    if not os.path.exists(ARQUIVO_CONFIG): return []
    try:
        with open(ARQUIVO_CONFIG, 'r', encoding='utf-8') as f: return json.load(f)
    except: return []

# --- GESTÃO DA DIMENSÃO DE TIMES (CACHE) ---
def carregar_cache_times(client):
    print("📚 Carregando cache de times (dim_times)...")
    cache = {}
    try:
        query = f"SELECT time_id, nome, slug, escudo FROM `{client.project}.{TAB_DIM_TIMES}`" # nosec B608
        df = client.query(query).to_dataframe()
        for _, row in df.iterrows():
            cache[str(row['time_id'])] = {
                'nome': row['nome'], 
                'slug': row['slug'], 
                'escudo': row['escudo']
            }
        print(f"📚 {len(cache)} times carregados do cache.")
    except Exception:
        print("⚠️ Tabela dim_times ainda não existe ou está vazia.")
    return cache

def salvar_novos_times(client, lista_novos):
    if not lista_novos: return
    print(f"💾 Salvando {len(lista_novos)} novos times na dimensão...")
    df = pd.DataFrame(lista_novos).drop_duplicates(subset=['time_id'])
    
    schema = [
        bigquery.SchemaField("time_id", "STRING"),
        bigquery.SchemaField("nome", "STRING"),
        bigquery.SchemaField("slug", "STRING"),
        bigquery.SchemaField("escudo", "STRING")
    ]
    
    job_config = bigquery.LoadJobConfig(
        schema=schema, 
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    try:
        client.load_table_from_dataframe(df, TAB_DIM_TIMES, job_config=job_config).result()
        print("✅ Novos times registrados com sucesso!")
    except Exception as e: 
        print(f"❌ Erro ao salvar novos times: {e}")

def buscar_dados_time_api(time_id):
    try:
        url = f"https://api.cartola.globo.com/time/id/{time_id}"
        res = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
        if res.status_code == 200:
            t = res.json().get('time', {})
            return {
                'time_id': str(time_id), 
                'nome': t.get('nome', f"Time {time_id}"),
                'slug': t.get('slug', ''), 
                'escudo': t.get('url_escudo_png', '')
            }
    except: pass
    return None

# --- FUNÇÕES DE COLETA DE DADOS ---
def limpar_dados_da_copa(client, slug):
    try:
        query = f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{slug}'" # nosec B608
        client.query(query).result()
    except: pass

def caçar_jogos_recursivo(dados):
    jogos = []
    if isinstance(dados, dict):
        if 'time_mandante_id' in dados: return [dados]
        for v in dados.values(): jogos.extend(caçar_jogos_recursivo(v))
    elif isinstance(dados, list):
        for item in dados: jogos.extend(caçar_jogos_recursivo(item))
    return jogos

# --- FLUXO PRINCIPAL ---
def coletar_dados_copa():
    copas = carregar_configuracao()
    token = get_token()
    headers = {'Authorization': f'Bearer {token}', 'User-Agent': 'Mozilla/5.0'}
    client = get_bq_client()
    ts = datetime.now(pytz.timezone('America/Sao_Paulo'))
    
    # Obtém o status global real do mercado
    try:
        st = requests.get("https://api.cartola.globo.com/mercado/status", headers={'User-Agent': 'Mozilla/5.0'}, timeout=30).json()
        status_mercado = st.get('status_mercado')
        rodada_global = (st.get('rodada_atual') - 1) if status_mercado == 1 else st.get('rodada_atual')
    except Exception as e:
        print(f"⚠️ Erro ao buscar status do mercado: {e}")
        return

    # Busca as parciais apenas se a rodada estiver rolando (mercado fechado)
    m_pts = cartola_utils.buscar_parciais_globais(headers) if status_mercado != 1 else {}
    m_sts = cartola_utils.buscar_status_partidas(headers) if status_mercado != 1 else {}

    cache_times = carregar_cache_times(client)
    novos_times_buffer = []

    for copa in copas:
        if not copa.get('ativa'): continue
        
        try:
            print(f"🏆 Processando: {copa['slug']}")
            res = requests.get(f"https://api.cartola.globo.com/auth/liga/{copa['slug']}", headers=headers, timeout=30).json()
            
            raw_t = res.get('times', [])
            dic_t_liga = {str(t.get('time_id') or t.get('id')): t for t in raw_t} if isinstance(raw_t, list) else raw_t
            
            jogos = caçar_jogos_recursivo(res.get('chaves_mata_mata', {}))
            l_final = []
            
            for j in jogos:
                id_a = str(j.get('time_mandante_id'))
                id_b = str(j.get('time_visitante_id'))
                
                times_jogo = {}
                for tid in [id_a, id_b]:
                    if tid in cache_times:
                        times_jogo[tid] = cache_times[tid]
                    elif tid in dic_t_liga:
                        dados_liga = dic_t_liga[tid]
                        obj_time = {
                            'time_id': tid, 'nome': dados_liga.get('nome'),
                            'slug': dados_liga.get('slug'), 'escudo': dados_liga.get('url_escudo_png')
                        }
                        times_jogo[tid] = obj_time
                        if tid not in [x['time_id'] for x in novos_times_buffer]:
                            novos_times_buffer.append(obj_time)
                            cache_times[tid] = obj_time 
                    else:
                        obj_api = buscar_dados_time_api(tid)
                        if obj_api:
                            times_jogo[tid] = obj_api
                            if tid not in [x['time_id'] for x in novos_times_buffer]:
                                novos_times_buffer.append(obj_api)
                                cache_times[tid] = obj_api
                        else:
                            times_jogo[tid] = {'nome': f"Time {tid}", 'slug': '', 'escudo': ''}
                
                t_a = times_jogo.get(id_a)
                t_b = times_jogo.get(id_b)

                r_jogo = j.get('rodada_id')
                
                # Se o jogo pertence à rodada atual do campeonato E o mercado está fechado, calcula as parciais
                if str(r_jogo) == str(rodada_global) and status_mercado != 1 and m_pts:
                    pts_a = cartola_utils.calcular_parciais_equipe(id_a, m_pts, m_sts, headers)[0]
                    pts_b = cartola_utils.calcular_parciais_equipe(id_b, m_pts, m_sts, headers)[0]
                else:
                    pts_a = float(j.get('time_mandante_pontuacao') or 0.0)
                    pts_b = float(j.get('time_visitante_pontuacao') or 0.0)

                l_final.append({
                    'nome_copa': copa['nome_visual'], 'liga_slug': copa['slug'], 'rodada_real': r_jogo,
                    'fase_copa': MAPA_FASES.get(str(j.get('tipo_fase')), 'Fase'),
                    'time_a_nome': t_a['nome'], 'time_a_slug': t_a['slug'], 'time_a_pontos': pts_a,
                    'time_b_nome': t_b['nome'], 'time_b_slug': t_b['slug'], 'time_b_pontos': pts_b,
                    'vencedor': dic_t_liga.get(str(j.get('vencedor_id')), {}).get('slug'), 'data_coleta': ts
                })
                
            if l_final:
                limpar_dados_da_copa(client, copa['slug'])
                client.load_table_from_dataframe(pd.DataFrame(l_final), TAB_COPA).result()
                print(f"✅ Copa {copa['slug']} atualizada com {len(l_final)} jogos.")
        except Exception as e: print(f"❌ Erro na liga {copa['slug']}: {e}")

    salvar_novos_times(client, novos_times_buffer)

if __name__ == "__main__":
    coletar_dados_copa()
