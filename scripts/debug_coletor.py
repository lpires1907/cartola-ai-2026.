import os
import json
import requests
from dotenv import load_dotenv

# --- CONFIG ---
SLUG_LIGA = "sas-brasil-2026" # Sua liga principal

def get_token():
    try:
        load_dotenv()
    except: pass
    return os.getenv("CARTOLA_GLBID")

def debug_liga():
    token = get_token()
    if not token:
        print("❌ Sem token. Configure o .env ou Secrets.")
        return

    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Authorization': f'Bearer {token}'
    }

    url = f"https://api.cartola.globo.com/auth/liga/{SLUG_LIGA}"
    print(f"🔬 Investigando Payload da Liga: {SLUG_LIGA}...\n")

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        dados = resp.json()

        # 1. Verifica dados gerais da liga
        if 'liga' in dados:
            print(f"✅ Liga encontrada: {dados['liga'].get('nome')}")
            print(f"🔄 Rodada Atual na API: {dados['liga'].get('rodada_atual')}")
        
        # 2. Analisa o primeiro time da lista (para ver os campos de pontos)
        if 'times' in dados and len(dados['times']) > 0:
            time_exemplo = dados['times'][0]
            print(f"\n🕵️‍♂️ Analisando time: {time_exemplo.get('nome')}")
            
            # Imprime TODOS os campos que tenham "ponto", "score", "parcial" no nome
            campos_suspeitos = {}
            for chave, valor in time_exemplo.items():
                if any(x in chave.lower() for x in ['ponto', 'score', 'parcial', 'rodada']):
                    campos_suspeitos[chave] = valor
            
            print("📊 Campos de Pontuação Encontrados:")
            print(json.dumps(campos_suspeitos, indent=4, ensure_ascii=False))
            
            # Imprime o JSON completo do time para você vasculhar com os olhos
            print("\n📋 JSON Completo do Time (Amostra):")
            print(json.dumps(time_exemplo, indent=2, ensure_ascii=False))
            
        else:
            print("⚠️ Nenhuma lista de 'times' encontrada no payload.")

        # 3. Teste Extra: Endpoint de Parciais Globais
        # Vamos ver se conseguimos pegar as parciais dos atletas para comparar
        url_parciais = "https://api.cartola.globo.com/atletas/pontuados"
        resp_parciais = requests.get(url_parciais, headers=headers, timeout=30)
        if resp_parciais.status_code == 200:
            total_atletas = len(resp_parciais.json().get('atletas', {}))
            print(f"\n✅ API de Parciais (/atletas/pontuados) está ativa: {total_atletas} atletas pontuaram.")
        else:
            print(f"\n⚠️ API de Parciais inacessível: {resp_parciais.status_code}")

    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    debug_liga()
