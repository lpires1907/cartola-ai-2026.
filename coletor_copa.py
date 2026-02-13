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

# --- FUNÇÕES DE INTELIGÊNCIA ---

def buscar_parciais_globais(headers):
    """Baixa pontuações de TODOS os atletas."""
    url = "https://api.cartola.globo.com/atletas/pontuados"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 200:
            dados = resp.json()
            mapa = {}
            atletas = dados.get('atletas', {})
            for id_str, info in atletas.items():
                mapa[int(id_str)] = info.get('pontuacao', 0.0)
            print(f"      📡 Parciais: {len(mapa)} atletas pontuados.")
            return mapa
    except Exception as e:
        print(f"      ⚠️ Erro parciais: {e}")
    return {}

def buscar_status_partidas(headers):
    """Mapeia o status do jogo de cada clube."""
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
            print(f"      ⚽ Status mapeados: {len(partidas)} partidas.")
    except Exception as e:
        print(f"      ⚠️ Erro status: {e}")
    return mapa_status

def calcular_pontuacao_completa(time_id, mapa_pontos, mapa_status_jogos, headers):
    """
    Calcula pontuação aplicando:
    1. Substituição Padrão (quem não jogou).
    2. Substituição de Luxo (melhor desempenho).
    3. Capitão.
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
        reserva_luxo_id = dados.get('reserva_luxo_id') # <--- A CHAVE DE OURO
        
        # Monta lista de trabalho com pontuações atuais
        titulares_ativos = []
        for t in titulares_raw:
            atleta_id = t['atleta_id']
            titulares_ativos.append({
                'atleta_id': atleta_id,
                'posicao_id': t['posicao_id'],
                'clube_id': t['clube_id'],
                'pontos': mapa_pontos.get(atleta_id, 0.0),
                'is_capitao': (atleta_id == capitao_id),
                'apelido': t['apelido']
            })

        # ==========================================================
        # FASE 1: SUBSTITUIÇÃO PADRÃO (TAPA-BURACO)
        # ==========================================================
        # Troca titulares que zeraram (não jogaram) pelo reserva da posição
        
        for i, titular in enumerate(titulares_ativos):
            status_jogo = mapa_status_jogos.get(titular['clube_id'], 'PRE_JOGO')
            
            # Se jogo acabou e titular zerou -> Tenta substituir
            if status_jogo == "ENCERRADA" and titular['pontos'] == 0.0:
                
                # Busca reserva da mesma posição que NÃO seja o de Luxo (prioriza banco comum primeiro)
                # ou usa qualquer um, já que o Luxo também pode tapar buraco.
                reserva_match = next((r for r in reservas_raw 
                                      if r['posicao_id'] == titular['posicao_id'] 
                                      and mapa_pontos.get(r['atleta_id'], 0.0) != 0.0), None)
                
                if reserva_match:
                    r_id = reserva_match['atleta_id']
                    r_pts = mapa_pontos.get(r_id, 0.0)
                    
                    # Realiza a troca
                    # print(f"  🔄 Sub Padrão: Sai {titular['apelido']} (0.0), Entra {reserva_match['apelido']} ({r_pts})")
                    titulares_ativos[i]['atleta_id'] = r_id
                    titulares_ativos[i]['pontos'] = r_pts
                    titulares_ativos[i]['clube_id'] = reserva_match['clube_id']
                    titulares_ativos[i]['apelido'] = reserva_match['apelido']
                    
                    # Remove esse reserva da lista para não usar de novo
                    reservas_raw.remove(reserva_match)

        # ==========================================================
        # FASE 2: RESERVA DE LUXO (DESEMPENHO)
        # ==========================================================
        
        if reserva_luxo_id:
            # Encontra o objeto do atleta de luxo no banco (se ainda estiver lá)
            luxo_obj = next((r for r in reservas_raw if r['atleta_id'] == reserva_luxo_id), None)
            
            if luxo_obj:
                luxo_pts = mapa_pontos.get(reserva_luxo_id, 0.0)
                pos_luxo = luxo_obj['posicao_id']
                
                # Filtra titulares da mesma posição do luxo
                concorrentes = [t for t in titulares_ativos if t['posicao_id'] == pos_luxo]
                
                if concorrentes:
                    # Verifica se TODOS os jogos dessa posição (titulares + luxo) acabaram
                    ids_clubes = [t['clube_id'] for t in concorrentes]
                    ids_clubes.append(luxo_obj['clube_id'])
                    
                    todos_encerrados = True
                    for cid in ids_clubes:
                        if mapa_status_jogos.get(cid) != "ENCERRADA":
                            todos_encerrados = False
                            break
                    
                    if todos_encerrados:
                        # Encontra o pior titular dessa posição
                        pior_titular = min(concorrentes, key=lambda x: x['pontos'])
                        
                        # A Regra: Se Luxo > Pior Titular, troca!
                        if luxo_pts > pior_titular['pontos']:
                            # print(f"  💎 Sub LUXO: Sai {pior_titular['apelido']} ({pior_titular['pontos']}), Entra {luxo_obj['apelido']} ({luxo_pts})")
                            
                            # Atualiza os dados do titular na lista principal
                            # (temos que achar o índice dele na lista original)
                            idx = titulares_ativos.index(pior_titular)
                            
                            titulares_ativos[idx]['atleta_id'] = reserva_luxo_id
                            titulares_ativos[idx]['pontos'] = luxo_pts
                            titulares_ativos[idx]['clube_id'] = luxo_obj['clube_id']
                            # Mantém a capitania se o titular era capitão (Regra comum: herda a braçadeira)

        # ==========================================================
        # FASE 3: SOMA FINAL E CAPITÃO
        # ==========================================================
        
        soma_total = 0.0
        for t in titulares_ativos:
            pts = t['pontos']
            if t['is_capitao']:
                soma_total += (pts * 1.5)
            else:
                soma_total += pts
        
        time.sleep(0.1) 
        return round(soma_total, 2)
            
    except Exception as e:
        print(f"      ⚠️ Erro calc time {time_id}: {e}")
        return 0.0

# --- FLUXO PRINCIPAL ---

def coletar_dados_copa():
    copas = carregar_configuracao()
    if not copas: return

    token = get_token()
    if not token:
        print("❌ ERRO: Token CARTOLA_GLBID não encontrado.")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    client = get_bq_client()
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))
    
    # 1. PREPARAÇÃO
    mapa_parciais = buscar_parciais_globais(headers)
    mapa_status = buscar_status_partidas(headers)
    
    print(f"🏆 Iniciando processamento de {len(copas)} copas...")

    for copa in copas:
        slug = copa.get('slug')
        nome_visual = copa.get('nome_visual')
        ativa = copa.get('ativa', False)

        if not ativa: continue
        
        print(f"   🔄 Processando: {nome_visual} ({slug})...")
        limpar_dados_da_copa(client, slug)

        url = f"https://api.cartola.globo.com/auth/liga/{slug}"
        
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            if resp.status_code != 200: continue

            dados = resp.json()
            rodada_atual = dados['liga'].get('rodada_atual', 0)
            dic_times = dados.get('times', {})
            raw_chaves = dados.get('chaves_mata_mata', {})
            
            todos_jogos = []
            if isinstance(raw_chaves, dict):
                iteravel = raw_chaves.values()
            elif isinstance(raw_chaves, list):
                iteravel = raw_chaves
            else:
                iteravel = []

            for item in iteravel:
                if isinstance(item, list): todos_jogos.extend(item)
                elif isinstance(item, dict): todos_jogos.append(item)

            print(f"      🔎 Encontrados {len(todos_jogos)} jogos. Calculando com Regras 2026...")

            lista_final = []
            
            for jogo in todos_jogos:
                try:
                    if not isinstance(jogo, dict): continue

                    id_mandante = str(jogo.get('time_mandante_id'))
                    id_visitante = str(jogo.get('time_visitante_id'))
                    id_vencedor = str(jogo.get('vencedor_id'))
                    
                    sigla_fase = jogo.get('tipo_fase', str(jogo.get('rodada_id', 'Fase Única')))
                    nome_fase = MAPA_FASES.get(sigla_fase, f"Rodada {sigla_fase}")

                    # --- CÁLCULO ---
                    pontos_a_api = float(jogo.get('time_mandante_pontuacao') or 0.0)
                    pontos_b_api = float(jogo.get('time_visitante_pontuacao') or 0.0)
                    
                    if mapa_parciais:
                        # Usa nossa calculadora turbinada (Padrão + Luxo)
                        pontos_a = calcular_pontuacao_completa(id_mandante, mapa_parciais, mapa_status, headers)
                        pontos_b = calcular_pontuacao_completa(id_visitante, mapa_parciais, mapa_status, headers)
                        
                        # Fallback de segurança
                        if pontos_a == 0.0 and pontos_a_api > 0: pontos_a = pontos_a_api
                        if pontos_b == 0.0 and pontos_b_api > 0: pontos_b = pontos_b_api
                    else:
                        pontos_a = pontos_a_api
                        pontos_b = pontos_b_api

                    # Dados Visuais
                    time_a = dic_times.get(id_mandante, {})
                    nome_a = time_a.get('nome', f'Time {id_mandante}')
                    escudo_a = time_a.get('url_escudo_png', '')
                    slug_a = time_a.get('slug', id_mandante)

                    time_b = dic_times.get(id_visitante, {})
                    nome_b = time_b.get('nome', f'Time {id_visitante}')
                    escudo_b = time_b.get('url_escudo_png', '')
                    slug_b = time_b.get('slug', id_visitante)
                    
                    slug_vencedor = None
                    if id_vencedor == id_mandante: slug_vencedor = str(slug_a)
                    elif id_vencedor == id_visitante: slug_vencedor = str(slug_b)

                    item = {
                        'nome_copa': nome_visual,
                        'liga_slug': slug,
                        'rodada_real': rodada_atual,
                        'fase_copa': nome_fase,
                        'time_a_nome': nome_a,
                        'time_a_slug': str(slug_a),
                        'time_a_escudo': escudo_a,
                        'time_a_pontos': pontos_a,
                        'time_b_nome': nome_b,
                        'time_b_slug': str(slug_b),
                        'time_b_escudo': escudo_b,
                        'time_b_pontos': pontos_b,
                        'vencedor': slug_vencedor,
                        'data_coleta': ts_agora
                    }
                    lista_final.append(item)
                except Exception as e:
                    print(f"      ⚠️ Erro jogo: {e}")

            if lista_final:
                df = pd.DataFrame(lista_final)
                schema = [
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
                ]
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    write_disposition="WRITE_APPEND",
                    schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
                )
                try:
                    client.load_table_from_dataframe(df, TAB_COPA, job_config=job_config).result()
                    print(f"      ✅ SUCESSO! {len(df)} jogos atualizados com REGRA DE LUXO.")
                except Exception as e:
                    print(f"      ❌ Erro BQ: {e}")
            else:
                print("      ⚠️ Nenhum jogo processado.")

        except Exception as e:
            print(f"      ❌ Erro fatal: {e}")

if __name__ == "__main__":
    coletar_dados_copa()
