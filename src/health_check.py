import os
import sys
import json
import requests
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Garante que o path de src seja considerado se rodar de fora
sys.path.append(os.path.join(os.getcwd(), 'src'))

def check_bq():
    print("🔍 [Health Check] Verificando BigQuery...")
    try:
        if os.getenv('GCP_SERVICE_ACCOUNT'):
            info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
            creds = service_account.Credentials.from_service_account_info(info)
            client = bigquery.Client(credentials=creds, project=info['project_id'])
        else:
            client = bigquery.Client()
        
        # Testa se consegue listar tabelas
        dataset_id = "cartola_analytics"
        tables = list(client.list_tables(f"{client.project}.{dataset_id}"))
        print(f"✅ BQ OK: {len(tables)} tabelas encontradas no dataset {dataset_id}.")
        
        # Verifica se a view crítica existe
        view_id = f"{client.project}.{dataset_id}.view_consolidada_times"
        client.get_table(view_id)
        print(f"✅ View Crítica OK: {view_id}")
        return True
    except Exception as e:
        print(f"❌ Erro no BigQuery: {e}")
        return False

def check_cartola_api():
    print("🔍 [Health Check] Verificando API do Cartola...")
    token = os.getenv("CARTOLA_GLBID")
    if not token:
        print("⚠️ Aviso: CARTOLA_GLBID não configurado (opcional para algumas rotas).")
    
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        if token: headers['Authorization'] = f'Bearer {token}'
        
        res = requests.get("https://api.cartola.globo.com/mercado/status", headers=headers, timeout=10)
        if res.status_code == 200:
            status = res.json().get('status_mercado')
            print(f"✅ API Cartola OK: Mercado status {status}.")
            return True
        else:
            print(f"❌ Erro API Cartola: Status {res.status_code}")
            return False
    except Exception as e:
        print(f"❌ Erro na API Cartola: {e}")
        return False

def main():
    print("🏥 INICIANDO HEALTH CHECK DO PROJETO\n")
    bq_ok = check_bq()
    api_ok = check_cartola_api()
    
    if bq_ok and api_ok:
        print("\n✨ [SUCCESS] Todos os sistemas operacionais.")
        sys.exit(0)
    else:
        print("\n🚨 [FAILURE] Falha em um ou mais componentes críticos.")
        sys.exit(1)

if __name__ == "__main__":
    load_dotenv()
    main()
