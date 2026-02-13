import os
import json
import requests
from dotenv import load_dotenv

# --- CONFIG ---
# ID de um time que sabemos que existe na sua liga (pegue do debug anterior)
TIME_ID_TESTE = 47972290  # Time "Lucas Moura > Pelé" (Exemplo)

def get_token():
    try:
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def debug_time_reservas():
    token = get_token()
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    url = f"https://api.cartola.globo.com/time/id/{TIME_ID_TESTE}"
    print(f"🔬 Investigando Time ID: {TIME_ID_TESTE}...\n")

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        dados = resp.json()

        # 1. Verifica Titulares
        atletas = dados.get('atletas', [])
        print(f"✅ Titulares encontrados: {len(atletas)}")

        # 2. Verifica Reservas (O Pulo do Gato)
        reservas = dados.get('reservas', [])
        print(f"✅ Reservas encontrados: {len(reservas)}")
        
        if reservas:
            print("\n📋 Exemplo de Reserva:")
            print(json.dumps(reservas[0], indent=2, ensure_ascii=False))
        else:
            print("\n⚠️ A lista de 'reservas' veio vazia ou não existe.")
            print(f"Chaves na raiz: {list(dados.keys())}")

    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    debug_time_reservas()
