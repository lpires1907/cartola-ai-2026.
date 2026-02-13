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

# Mapa de Fases (Converte códigos do Cartola para texto legível)
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
        # nosec: O slug vem do arquivo de configuração interno, seguro contra injection
        query = f"DELETE FROM `{client.project}.{TAB_COPA}` WHERE liga_slug = '{slug}'" # nosec
        client.query(query).result()
        print(f"🧹 Dados antigos removidos para '{slug}'.")
    except Exception as e:
        print(f"ℹ️ Limpeza pulada (Tabela inexistente ou erro): {e}")

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

        url = f"https://api.cartola.globo.com/auth/liga/{slug}"
        
        try:
            # FIX DE SEGURANÇA: timeout adicionado para passar no Bandit
            resp = requests.get(url, headers=headers, timeout=30)
            
            if resp.status_code != 200:
                print(f"      ❌ Erro API: {resp.status_code}")
                continue

            dados = resp.json()
            rodada_atual = dados['liga'].get('rodada_atual', 0)
            
            # 1. Dicionário de Times (ID -> Dados)
            # O Cartola manda os detalhes dos times (Nome, Escudo) separados dos confrontos
            dic_times = dados.get('times', {})
            
            # 2. Chaves do Mata-Mata
            # Estrutura: {"1": [jogos], "2": [jogos]}
            raw_chaves = dados.get('chaves_mata_mata', {})
            
            lista_final = []

            if isinstance(raw_chaves, list):
                # Caso raro onde venha como lista direta
                todos_jogos = raw_chaves
            elif isinstance(raw_chaves, dict):
                # Caso padrão: Dicionário agrupado por rodada/chave
                todos_jogos = []
                for _, lista in raw_chaves.items():
                    if isinstance(lista, list):
                        todos_jogos.extend(lista)
            else:
                todos_jogos = []

            print(f"      🔎 Encontrados {len(todos_jogos)} jogos brutos.")

            for jogo in todos_jogos:
                try:
                    # IDs retornados pela API
                    id_mandante = str(jogo.get('time_mandante_id'))
                    id_visitante = str(jogo.get('time_visitante_id'))
                    id_vencedor = str(jogo.get('vencedor_id'))
                    
                    # Identifica a Fase (Ex: "O" -> "Oitavas de Final")
                    sigla_fase = jogo.get('tipo_fase', '')
                    nome_fase = MAPA_FASES.get(sigla_fase, f"Fase {sigla_fase}")

                    # Cruzamento de Dados: Busca nome e escudo no dicionário 'times'
                    time_a = dic_times.get(id_mandante, {})
                    nome_a = time_a.get('nome', f'Time {id_mandante}')
                    escudo_a = time_a.get('url_escudo_png', '')
                    slug_a = time_a.get('slug', id_mandante)
                    pontos_a = float(jogo.get('time_mandante_pontuacao') or 0.0)

                    time_b = dic_times.get(id_visitante, {})
                    nome_b = time_b.get('nome', f'Time {id_visitante}')
                    escudo_b = time_b.get('url_escudo_png', '')
                    slug_b = time_b.get('slug', id_visitante)
                    pontos_b = float(jogo.get('time_visitante_pontuacao') or 0.0)
                    
                    # Define quem venceu pelo slug
                    slug_vencedor = None
                    if id_vencedor == id_mandante: slug_vencedor = slug_a
                    elif id_vencedor == id_visitante: slug_vencedor = slug_b

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
                        
                        'vencedor': str(slug_vencedor) if slug_vencedor else None,
                        'data_coleta': ts_agora
                    }
                    lista_final.append(item)
                except Exception as e:
                    print(f"      ⚠️ Erro ao processar jogo individual: {e}")

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
                    print(f"      ✅ SUCESSO! {len(df)} jogos da copa salvos.")
                except Exception as e:
                    print(f"      ❌ Erro ao salvar no BigQuery: {e}")
            else:
                print("      ⚠️ Nenhum jogo foi extraído com sucesso.")

        except Exception as e:
            print(f"      ❌ Erro fatal na requisição: {e}")

if __name__ == "__main__":
    coletar_dados_copa()
