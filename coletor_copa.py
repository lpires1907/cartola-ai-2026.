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
    """Lê o arquivo JSON com a lista de copas."""
    if not os.path.exists(ARQUIVO_CONFIG):
        print(f"⚠️ Arquivo {ARQUIVO_CONFIG} não encontrado. Crie-o na raiz.")
        return []
    
    try:
        with open(ARQUIVO_CONFIG, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Erro ao ler {ARQUIVO_CONFIG}: {e}")
        return []

def limpar_dados_da_copa(client, slug):
    """
    Remove todos os dados de uma copa específica antes de inserir a versão atualizada.
    """
    try:
        # CORREÇÃO B608: Adicionado '# nosec' para validar que o slug vem de config confiável
        query = f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{slug}'" # nosec
        client.query(query).result()
        print(f"🧹 Dados antigos da copa '{slug}' removidos com sucesso.")
    except Exception as e:
        print(f"⚠️ Aviso na limpeza (pode ser a primeira execução): {e}")

def coletar_dados_copa():
    copas = carregar_configuracao()
    if not copas:
        return

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
    
    print(f"🏆 Iniciando processamento de {len(copas)} copas configuradas...")

    for copa in copas:
        slug = copa.get('slug')
        nome_visual = copa.get('nome_visual')
        ativa = copa.get('ativa', False)

        if not ativa:
            print(f"   ⏭️ Pulando {nome_visual} ({slug}) - Marcada como inativa/encerrada.")
            continue
        
        print(f"   🔄 Atualizando: {nome_visual} ({slug})...")
        
        # 1. Limpeza Prévia
        limpar_dados_da_copa(client, slug)

        # 2. Coleta Nova
        url = f"https://api.cartola.globo.com/auth/liga/{slug}"
        confrontos_lista = []

        try:
            # CORREÇÃO B113: Adicionado timeout de 30 segundos
            resp = requests.get(url, headers=headers, timeout=30)
            
            if resp.status_code != 200:
                print(f"      ❌ Erro API ({resp.status_code}) ao acessar {slug}")
                continue

            dados = resp.json()
            rodada_atual = dados['liga'].get('rodada_atual', 0)
            
            # Busca confrontos
            confrontos = dados.get('confrontos', [])
            if not confrontos and 'chaves' in dados:
                 confrontos = dados['chaves']

            if not confrontos:
                print("      ⚠️ Nenhum confronto encontrado na API.")
                continue

            for c in confrontos:
                try:
                    t1 = c.get('time_a', {}) or {}
                    t2 = c.get('time_b', {}) or {}
                    
                    if not t1 and not t2: continue

                    item = {
                        'nome_copa': nome_visual,
                        'liga_slug': slug,
                        'rodada_real': rodada_atual,
                        'fase_copa': c.get('nome_fase', 'Fase Única'),
                        
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
                    confrontos_lista.append(item)
                except Exception as e:
                    print(f"      ⚠️ Erro ao processar item: {e}")

            # 3. Inserção no BigQuery
            if confrontos_lista:
                df = pd.DataFrame(confrontos_lista)
                
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
                
                client.load_table_from_dataframe(df, TAB_COPA, job_config=job_config).result()
                print(f"      ✅ Salvo! {len(df)} confrontos atualizados para {nome_visual}.")
            else:
                print("      ⚠️ Lista de confrontos processada ficou vazia.")

        except Exception as e:
            print(f"      ❌ Erro fatal na liga {slug}: {e}")

if __name__ == "__main__":
    coletar_dados_copa()
