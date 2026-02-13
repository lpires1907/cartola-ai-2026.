import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv

def get_token():
    try:
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def debug_partidas():
    token = get_token()
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    # Endpoint que lista os jogos da rodada atual
    url = "https://api.cartola.globo.com/partidas"
    print(f"🔬 Investigando Jogos da Rodada...\n")

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        dados = resp.json()
        
        partidas = dados.get('partidas', [])
        print(f"✅ Encontradas {len(partidas)} partidas.")

        if partidas:
            # Pega o primeiro jogo para analisarmos a estrutura
            jogo = partidas[0]
            print(f"\n🏟️ Exemplo de Jogo: {jogo.get('clube_casa_id')} x {jogo.get('clube_visitante_id')}")
            
            # Imprime campos que indicam STATUS
            print("📊 Campos de Status Encontrados:")
            campos_status = {k: v for k, v in jogo.items() if any(x in k for x in ['status', 'transmissao', 'periodo', 'valida', 'placar'])}
            print(json.dumps(campos_status, indent=4, ensure_ascii=False))
            
            print("\n📋 JSON Completo do Jogo (Amostra):")
            print(json.dumps(jogo, indent=2, ensure_ascii=False))

    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    debug_partidas()
