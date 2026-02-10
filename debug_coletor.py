import os
import json
import requests
from datetime import datetime

# --- CONFIG ---
SLUG_COPA = "1a-copa-sas-brasil-2026"
URLS_TESTE = [
    f"https://api.cartola.globo.com/auth/liga/{SLUG_COPA}",
    f"https://api.cartola.globo.com/auth/liga/{SLUG_COPA}/mata-mata"
]

def get_token():
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def debug():
    token = get_token()
    if not token:
        print("❌ Sem token CARTOLA_GLBID.")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    print(f"🕵️‍♂️ INICIANDO DIAGNÓSTICO DA LIGA: {SLUG_COPA}\n")

    for url in URLS_TESTE:
        print(f"--- Testando URL: {url} ---")
        try:
            # CORREÇÃO B113: Adicionado timeout de 30 segundos
            resp = requests.get(url, headers=headers, timeout=30)
            
            if resp.status_code != 200:
                print(f"❌ Erro {resp.status_code}")
                continue
                
            dados = resp.json()
            print("✅ JSON Recebido com sucesso!")
            print(f"🔑 Chaves na raiz do JSON: {list(dados.keys())}")
            
            if 'liga' in dados:
                print(f"📂 Dentro de ['liga']: {list(dados['liga'].keys())}")
                if 'mata_mata' in dados['liga']:
                     print("   ⚠️ ACHEI! Existe ['liga']['mata_mata']")
            
            if 'mata_mata' in dados:
                print(f"📂 Dentro de ['mata_mata']: {list(dados['mata_mata'].keys())}")

            if 'confrontos' in dados:
                print(f"📂 Dentro de ['confrontos']: Encontrados {len(dados['confrontos'])} itens.")
                
            if 'chaves' in dados:
                 print(f"📂 Dentro de ['chaves']: Encontrados {len(dados['chaves'])} itens.")
            
            print("\n" + "="*40 + "\n")
        
        except Exception as e:
            print(f"❌ Erro na requisição: {e}")

if __name__ == "__main__":
    debug()
