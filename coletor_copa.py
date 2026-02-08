import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES ---
LIGA_SLUG = "1a-copa-sas-brasil-2026"  # Slug da sua Copa
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
    # Tenta pegar do arquivo .env ou variável de ambiente
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_TOKEN")

def coletar_dados_copa():
    print(f"🏆 Iniciando coleta da Copa: {LIGA_SLUG}")
    
    token = get_token()
    if not token:
        print("❌ ERRO: Token do Cartola não encontrado (CARTOLA_TOKEN).")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'X-GLB-Token': token
    }

    url = f"https://api.cartola.globo.com/auth/liga/{LIGA_SLUG}"
    
    try:
        resp = requests.get(url, headers=headers)
        
        if resp.status_code != 200:
            print(f"❌ Erro na API: {resp.status_code} - {resp.text}")
            return

        dados = resp.json()
        
        # Verifica se é Mata-Mata
        if 'mata_mata' in dados['liga'] and not dados['liga']['mata_mata']:
            print("⚠️ Aviso: A API diz que essa liga NÃO é mata-mata. Verifique o slug.")
        
        # Extrair confrontos (A estrutura pode variar se for chaveamento ou lista)
        # Geralmente mata-mata retorna 'confrontos' ou chaves dentro de 'mata_mata'
        confrontos_lista = []
        
        # Estratégia de extração (Adaptável)
        rodada_atual_copa = dados['liga'].get('rodada_atual', 0)
        
        # Tenta pegar confrontos diretos (se disponível no endpoint principal)
        confrontos = dados.get('confrontos', [])
        
        # Se não vier direto, as vezes vem dentro de um objeto 'chaves'
        if not confrontos and 'chaves' in dados:
             confrontos = dados['chaves']

        print(f"🔎 Encontrados {len(confrontos)} confrontos na estrutura.")

        ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))

        for c in confrontos:
            # Estrutura típica de confronto:
            # time_a: { time_id, nome, ... }, time_b: { ... }, placar_a, placar_b
            try:
                t1 = c.get('time_a', {}) or {}
                t2 = c.get('time_b', {}) or {}
                
                # Ignora placeholders vazios
                if not t1 and not t2: continue

                item = {
                    'liga_slug': LIGA_SLUG,
                    'rodada_real': rodada_atual_copa, # Rodada do Cartola
                    'fase_copa': c.get('nome_fase', 'Rodada'), # Ex: Oitavas, Quartas...
                    
                    'time_a_nome': t1.get('nome', 'Indefinido'),
                    'time_a_slug': t1.get('slug', ''),
                    'time_a_pontos': float(c.get('pontuacao_a', 0) or 0),
                    
                    'time_b_nome': t2.get('nome', 'Indefinido'),
                    'time_b_slug': t2.get('slug', ''),
                    'time_b_pontos': float(c.get('pontuacao_b', 0) or 0),
                    
                    'vencedor': c.get('vencedor', {}).get('slug') if c.get('vencedor') else None,
                    'data_coleta': ts_agora
                }
                confrontos_lista.append(item)
            except Exception as e:
                print(f"⚠️ Erro ao processar um confronto: {e}")

        if not confrontos_lista:
            print("⚠️ Nenhum confronto extraído. O JSON pode ter mudado.")
            # Dica: Salvar o JSON bruto para debug se necessário
            return

        # Salvar no BigQuery
        df = pd.DataFrame(confrontos_lista)
        client = get_bq_client()
        
        # Schema para garantir tipos
        schema = [
            bigquery.SchemaField("liga_slug", "STRING"),
            bigquery.SchemaField("rodada_real", "INTEGER"),
            bigquery.SchemaField("fase_copa", "STRING"),
            bigquery.SchemaField("time_a_nome", "STRING"),
            bigquery.SchemaField("time_a_slug", "STRING"),
            bigquery.SchemaField("time_a_pontos", "FLOAT"),
            bigquery.SchemaField("time_b_nome", "STRING"),
            bigquery.SchemaField("time_b_slug", "STRING"),
            bigquery.SchemaField("time_b_pontos", "FLOAT"),
            bigquery.SchemaField("vencedor", "STRING"),
            bigquery.SchemaField("data_coleta", "TIMESTAMP"),
        ]
        
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND", # Vai acumulando histórico
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="data_coleta"
            )
        )
        
        client.load_table_from_dataframe(df, TAB_COPA, job_config=job_config).result()
        print(f"✅ Sucesso! {len(df)} confrontos salvos em {TAB_COPA}")

    except Exception as e:
        print(f"❌ Erro fatal no coletor da copa: {e}")

if __name__ == "__main__":
    coletar_dados_copa()
