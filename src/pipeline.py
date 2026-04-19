import os
import sys
# Importa os módulos do projeto
import coletor
import coletor_copa  # <--- 1. IMPORT NOVO
import processamento
import narrador

def main():
    print("🚀 INICIANDO PIPELINE CARTOLA ANALYTICS")
    
    # --- ETAPA 1: COLETA DE DADOS (LIGA CLÁSSICA) ---
    print("\n--- ETAPA 1: COLETA LIGA PONTOS CORRIDOS ---")
    # Roda a coleta (que agora usa .env local ou Secrets na nuvem)
    coletor.rodar_coleta()

    # --- ETAPA 1.5: COLETA DE DADOS (COPAS MATA-MATA) ---
    print("\n--- ETAPA 1.5: COLETA COPAS MATA-MATA ---")
    try:
        # 2. EXECUÇÃO NOVA: Roda o coletor genérico de copas
        coletor_copa.coletar_dados_copa()
    except Exception as e:
        # Usamos try/except para que um erro na Copa não trave a atualização da Liga Principal
        print(f"⚠️ Erro não bloqueante na coleta da Copa: {e}")
    
    # Obtém o cliente do BigQuery reutilizando a lógica do coletor
    try:
        client = coletor.get_bq_client()
        dataset_id = coletor.DATASET_ID
    except Exception as e:
        print(f"❌ Erro crítico ao obter cliente BigQuery: {e}")
        return

    # --- ETAPA 2: PROCESSAMENTO E CURA ---
    print("\n--- ETAPA 2: PROCESSAMENTO ---")
    
    # A) Atualiza Tabela Mensal (Metadados de campeões)
    try:
        processamento.atualizar_campeoes_mensais(client, dataset_id)
    except Exception as e:
        print(f"⚠️ Erro ao atualizar campeões mensais: {e}")
    
    # B) RECRIAR A VIEW (CRÍTICO: Isso conserta o Streamlit)
    try:
        processamento.recriar_view_consolidada(client, dataset_id)
    except Exception as e:
        print(f"⚠️ Erro ao recriar view consolidada: {e}")
    
    # --- ETAPA 3: NARRADOR (IA) ---
    print("\n--- ETAPA 3: NARRADOR IA ---")
    try:
        narrador.gerar_narracao_rodada()
    except Exception as e:
        print(f"⚠️ Erro no Narrador (Não bloqueante): {e}")
    
    print("\n✅ Pipeline executada com sucesso!")

if __name__ == "__main__":
    main()
