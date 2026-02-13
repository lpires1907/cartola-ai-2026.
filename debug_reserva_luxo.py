import os
import json
import requests
from dotenv import load_dotenv

# --- CONFIG ---
# Use o ID de um time que você sabe que escalou reservas
# (Pode usar o mesmo do teste anterior se ele tiver time completo)
TIME_ID_ALVO = 47972290 

def get_token():
    try:
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def debug_reserva_luxo():
    token = get_token()
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    url = f"https://api.cartola.globo.com/time/id/{TIME_ID_ALVO}"
    print(f"🔬 Investigando Reserva de Luxo no Time: {TIME_ID_ALVO}...\n")

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        dados = resp.json()

        # 1. Verifica chaves na raiz (pode ter algo novo sobre regras)
        print(f"🔑 Chaves Raiz: {list(dados.keys())}")
        
        # 2. Analisa os Reservas detalhadamente
        reservas = dados.get('reservas', [])
        print(f"\n✅ Reservas encontrados: {len(reservas)}")
        
        if reservas:
            print("\n📋 Detalhes do Primeiro Reserva (Busca por 'luxo', 'ordem', 'tipo'):")
            reserva = reservas[0]
            
            # Imprime o JSON bonito para lermos tudo
            print(json.dumps(reserva, indent=4, ensure_ascii=False))
            
            # Varredura extra por campos suspeitos em TODOS os reservas
            campos_estranhos = set()
            for r in reservas:
                for k in r.keys():
                    if k not in ['atleta_id', 'pontos_num', 'apelido', 'posicao_id', 'clube_id', 'status_id']:
                        campos_estranhos.add(k)
            
            print(f"\n🧐 Campos 'Extras' encontrados nos reservas: {list(campos_estranhos)}")

        else:
            print("⚠️ Esse time não escalou reservas.")

    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    debug_reserva_luxo()
