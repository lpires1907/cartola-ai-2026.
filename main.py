import os
import sys
# Importa os módulos do projeto
import coletor
import processamento
import narrador

def main():
    print("🚀 INICIANDO PIPELINE CARTOLA ANALYTICS")
    
    # --- ETAPA 1: COLETA DE DADOS ---
    print("\n--- ETAPA 1: COLETA ---")
    # Roda a coleta (que agora usa .env local ou Secrets na nuvem)
    coletor.rodar_coleta()
    
    # Obtém o cliente do BigQuery reutilizando a lógica do coletor
    # Isso garante que estamos usando as mesmas credenciais que funcionaram na coleta
    try:
        client = coletor.get_bq_client()
        dataset_id = coletor.DATASET_ID
    except Exception as e:
        print(f"❌ Erro crítico ao obter cliente BigQuery: {e}")
        return

    # --- ETAPA 2: PROCESSAMENTO E CURA ---
    print("\n--- ETAPA 2: PROCESSAMENTO ---")
    
    # A) Atualiza Tabela Mensal (Metadados de campeões)
    processamento.atualizar_campeoes_mensais(client, dataset_id)
    
    # B) RECRIAR A VIEW (CRÍTICO: Isso conserta o Streamlit)
    # Garante que a View esteja limpa e sem duplicatas a cada execução
    processamento.recriar_view_consolidada(client, dataset_id)
    
    # --- ETAPA 3: NARRADOR (IA) ---
    print("\n--- ETAPA 3: NARRADOR IA ---")
    try:
        narrador.gerar_narracao_rodada()
    except Exception as e:
        print(f"⚠️ Erro no Narrador (Não bloqueante): {e}")
    
    print("\n✅ Pipeline executada com sucesso!")

if __name__ == "__main__":
    main()
