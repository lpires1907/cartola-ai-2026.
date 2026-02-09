import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES ---
ARQUIVO_CONFIG = "copas.json"
DATASET_ID = "cartola_analytics"
TAB_COPA = f"{DATASET_ID}.copa_mata_mata"

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
    if not os.path.exists(ARQUIVO_CONFIG):
        print(f"⚠️ Arquivo {ARQUIVO_CONFIG} não encontrado.")
        return []
    try:
        with open(ARQUIVO_CONFIG, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Erro ao ler {ARQUIVO_CONFIG}: {e}")
        return []

def limpar_dados_da_copa(client, slug):
    try:
        # nosec: slug vem de config interna
        query = f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{slug}'" # nosec
        client.query(query).result()
        print(f"🧹 Dados antigos removidos para '{slug}'.")
    except Exception as e:
        print(f"ℹ️ Limpeza pulada (Tabela inexistente ou erro): {e}")

def buscar_confrontos_na_api(slug, headers):
    """
    Tenta encontrar os confrontos usando a chave 'chaves_mata_mata' descoberta no debug.
    """
    url_padrao = f"https://api.cartola.globo.com/auth/liga/{slug}"
    print(f"      🔎 Consultando API: {url_padrao}")
    
    try:
        resp = requests.get(url_padrao, headers=headers, timeout=30)
        if resp.status_code == 200:
            dados = resp.json()
            rodada = dados['liga'].get('rodada_atual', 0)
            matches_encontrados = []

            # --- ESTRATÉGIA 1: 'chaves_mata_mata' (A que apareceu no Debug) ---
            if 'chaves_mata_mata' in dados:
                raw = dados['chaves_mata_mata']
                
                # Se for um Dicionário (ex: {"chave_1": {...}, "chave_2": {...}})
                if isinstance(raw, dict):
                    # Itera sobre as chaves do dicionário para achar os confrontos
                    for key, val in raw.items():
                        # Se o valor for um objeto com 'confrontos'
                        if isinstance(val, dict):
                            # Tenta pegar o nome da fase (ex: Final, Semifinal)
                            nome_fase = val.get('nome', f'Chave {key}')
                            
                            # Se tiver lista de confrontos dentro
                            if 'confrontos' in val:
                                for c in val['confrontos']:
                                    c['nome_fase_extraida'] = nome_fase
                                    matches_encontrados.append(c)
                            
                            # Se o próprio objeto já parecer um confronto (tem time_a)
                            elif 'time_a' in val:
                                val['nome_fase_extraida'] = nome_fase
                                matches_encontrados.append(val)

                # Se for uma Lista direto
                elif isinstance(raw, list):
                    matches_encontrados = raw

                if matches_encontrados:
                    print(f"      ✅ Sucesso! Encontrados em 'chaves_mata_mata'.")
                    return matches_encontrados, rodada

            # --- ESTRATÉGIA 2: Fallback padrão ---
            if 'confrontos' in dados and dados['confrontos']:
                return dados['confrontos'], rodada
            
            if 'chaves' in dados and dados['chaves']:
                return dados['chaves'], rodada

    except Exception as e:
        print(f"      ⚠️ Erro ao processar API: {e}")

    return [], 0

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
    
    print(f"🏆 Iniciando processamento de {len(copas)} copas...")

    for copa in copas:
        slug = copa.get('slug')
        nome_visual = copa.get('nome_visual')
        ativa = copa.get('ativa', False)

        if not ativa: continue
        
        print(f"   🔄 Processando: {nome_visual} ({slug})...")
        limpar_dados_da_copa(client, slug)

        # Busca com a nova lógica
        confrontos, rodada_api = buscar_confrontos_na_api(slug, headers)

        if not confrontos:
            print("      ❌ FALHA: Nenhum confronto encontrado.")
            continue

        print(f"      ✅ Total de {len(confrontos)} duelos para processar.")

        lista_final = []
        for c in confrontos:
            try:
                t1 = c.get('time_a') or {}
                t2 = c.get('time_b') or {}
                
                # Ignora placeholders vazios
                if not t1 and not t2: continue

                # Tenta pegar o nome da fase que extraímos ou usa o padrão
                fase = c.get('nome_fase_extraida') or c.get('nome_fase') or 'Fase Única'

                item = {
                    'nome_copa': nome_visual,
                    'liga_slug': slug,
                    'rodada_real': rodada_api,
                    'fase_copa': fase,
                    
                    'time_a_nome': t1.get('nome', 'A Definir'),
                    'time_a_slug': t1.get('slug', ''),
                    'time_a_escudo': t1.get('url_escudo_png', ''),
                    'time_a_pontos': float(c.get('pontuacao_a', 0) or 0),
                    
                    'time_b_nome': t2.get('nome', 'A Definir'),
                    'time_b_slug': t2.get('slug', ''),
                    'time_b_escudo': t2.get('url_escudo_png', ''),
                    'time_b_pontos': float(c.get('pontuacao_b', 0) or 0),
                    
                    'vencedor': c.get('vencedor', {}).get('slug') if c.get('vencedor') else None,
                    'data_coleta': ts_agora
                }
                lista_final.append(item)
            except Exception as e:
                print(f"      ⚠️ Erro ao processar item: {e}")

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
                print(f"      💾 SUCESSO! {len(df)} registros salvos no BigQuery.")
            except Exception as e:
                print(f"      ❌ Erro BQ: {e}")
        else:
            print("      ⚠️ Lista final vazia.")

if __name__ == "__main__":
    coletar_dados_copa()
