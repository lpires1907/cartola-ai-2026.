import os
import json
import requests
from dotenv import load_dotenv

# --- CONFIG ---
SLUG_COPA = "1a-copa-sas-brasil-2026"

def get_token():
    try:
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def debug():
    token = get_token()
    if not token:
        print("❌ Sem token. Verifique o .env ou Secrets.")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    url = f"https://api.cartola.globo.com/auth/liga/{SLUG_COPA}"
    print(f"🔬 Analisando estrutura profunda de: {SLUG_COPA}...\n")

    try:
        # Adicionei timeout para evitar erro do Bandit
        resp = requests.get(url, headers=headers, timeout=30)
        
        if resp.status_code != 200:
            print(f"❌ Erro API: {resp.status_code}")
            return

        dados = resp.json()

        if 'chaves_mata_mata' in dados:
            raw = dados['chaves_mata_mata']
            print(f"✅ 'chaves_mata_mata' encontrada. Tipo: {type(raw)}")
            
            print("\n📋 CONTEÚDO BRUTO (Copie o resultado abaixo):")
            # Imprime o JSON formatado para lermos os nomes dos campos
            print(json.dumps(raw, indent=2, ensure_ascii=False)[:4000]) 
            
        else:
            print("❌ 'chaves_mata_mata' NÃO encontrada neste request.")

    except Exception as e:
        print(f"❌ Erro fatal: {e}")

if __name__ == "__main__":
    debug()
