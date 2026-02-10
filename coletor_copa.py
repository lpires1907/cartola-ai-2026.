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

# --- ESTRATÉGIA RECURSIVA ---
def extrair_confrontos_recursivo(dados, nome_fase_pai=None):
    confrontos_achados = []

    if isinstance(dados, dict):
        fase_atual = dados.get('nome', nome_fase_pai)
        
        # TENTA IDENTIFICAR SE É UM CONFRONTO (Várias opções de chaves)
        # Opção 1: Padrão (time_a / time_b)
        if 'time_a' in dados and 'time_b' in dados:
            dados['nome_fase_extraida'] = fase_atual
            return [dados]
        
        # Opção 2: Mata-mata antigo (clube_casa_id / clube_visitante_id)
        if 'clube_casa_id' in dados and 'clube_visitante_id' in dados:
             # Normaliza para o formato padrão
             dados['time_a'] = {'id': dados['clube_casa_id'], 'slug': str(dados['clube_casa_id'])}
             dados['time_b'] = {'id': dados['clube_visitante_id'], 'slug': str(dados['clube_visitante_id'])}
             dados['nome_fase_extraida'] = fase_atual
             return [dados]

        # Continua buscando
        for key, value in dados.items():
            confrontos_achados.extend(extrair_confrontos_recursivo(value, fase_atual))

    elif isinstance(dados, list):
        for item in dados:
            confrontos_achados.extend(extrair_confrontos_recursivo(item, nome_fase_pai))

    return confrontos_achados

def buscar_confrontos_na_api(slug, headers):
    url_padrao = f"https://api.cartola.globo.com/auth/liga/{slug}"
    print(f"      🔎 Consultando API: {url_padrao}")
    
    try:
        resp = requests.get(url_padrao, headers=headers, timeout=30)
        if resp.status_code == 200:
            dados = resp.json()
            rodada = dados['liga'].get('rodada_atual', 0)
            
            alvo_busca = None
            origem = ""

            if 'chaves_mata_mata' in dados:
                origem = "chaves_mata_mata"
                alvo_busca = dados['chaves_mata_mata']
            elif 'confrontos' in dados:
                origem = "confrontos"
                alvo_busca = dados['confrontos']
            elif 'liga' in dados and 'mata_mata' in dados['liga']:
                 origem = "liga.mata_mata"
                 alvo_busca = dados['liga']['mata_mata']

            if alvo_busca:
                matches = extrair_confrontos_recursivo(alvo_busca)
                if matches:
                    return matches, rodada
                else:
                    print(f"      ⚠️ Chave '{origem}' encontrada, mas nenhum match compatível.")
                    print("      🕵️‍♂️ IMPRIMINDO CONTEÚDO PARA DEBUG (Copie isso):")
                    # Imprime apenas uma amostra para não poluir demais
                    try:
                        print(json.dumps(alvo_busca, indent=2, ensure_ascii=False)[:2000] + "...")
                    except:
                        print(alvo_busca)
            
    except Exception as e:
        print(f"      ⚠️ Erro API: {e}")

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

        confrontos, rodada_api = buscar_confrontos_na_api(slug, headers)

        if not confrontos:
            print("      ❌ FALHA: Nenhum confronto encontrado.")
            continue

        print(f"      ✅ Sucesso! {len(confrontos)} duelos encontrados.")

        lista_final = []
        for c in confrontos:
            try:
                # Tenta normalizar os dados
                t1 = c.get('time_a') or {}
                t2 = c.get('time_b') or {}
                
                # Se t1/t2 forem apenas IDs (inteiros), tenta converter para objeto mínimo
                if isinstance(t1, int): t1 = {'nome': f'Time {t1}', 'slug': str(t1)}
                if isinstance(t2, int): t2 = {'nome': f'Time {t2}', 'slug': str(t2)}

                # Garante que temos pelo menos um placeholder
                if not t1 and not t2: continue

                fase = c.get('nome_fase_extraida') or c.get('nome_fase') or c.get('nome') or 'Fase Única'

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
                print(f"      💾 Salvo no BigQuery: {len(df)} registros.")
            except Exception as e:
                print(f"      ❌ Erro BQ: {e}")
        else:
            print("      ⚠️ Lista final vazia.")

if __name__ == "__main__":
    coletar_dados_copa()
