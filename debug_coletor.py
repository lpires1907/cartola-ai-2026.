import os
import json
import requests

# --- CONFIG ---
SLUG_COPA = "1a-copa-sas-brasil-2026"

def get_token():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def debug():
    token = get_token()
    if not token:
        print("❌ Sem token. Verifique o .env")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    url = f"https://api.cartola.globo.com/auth/liga/{SLUG_COPA}"
    print(f"🔬 Analisando estrutura profunda de: {SLUG_COPA}...\n")

    try:
        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            print(f"❌ Erro API: {resp.status_code}")
            return

        dados = resp.json()

        if 'chaves_mata_mata' in dados:
            raw = dados['chaves_mata_mata']
            print(f"✅ 'chaves_mata_mata' encontrada. Tipo: {type(raw)}")
            
            # Se for dicionário, pega o primeiro item para ver a cara dele
            if isinstance(raw, dict):
                first_key = next(iter(raw))
                first_item = raw[first_key]
                print(f"\n🔎 Exemplo de Item (Chave {first_key}):")
                print(json.dumps(first_item, indent=4, ensure_ascii=False))
            
            # Se for lista
            elif isinstance(raw, list):
                if raw:
                    print("\n🔎 Exemplo de Item da Lista:")
                    print(json.dumps(raw[0], indent=4, ensure_ascii=False))
                else:
                    print("⚠️ A lista está vazia.")
        else:
            print("❌ 'chaves_mata_mata' NÃO encontrada neste request.")

    except Exception as e:
        print(f"❌ Erro fatal: {e}")

if __name__ == "__main__":
    debug()
