import sys
from coletor import rodar_coleta
from processamento import rodar_processamento
from narrador import rodar_narracao
# from exportador import rodar_exportacao  <-- COMENTADO (ou apagado)

def main():
    print("🚀 INICIANDO PIPELINE CARTOLA ANALYTICS")
    
    try:
        # 1. ETL
        rodar_coleta()
        
        # 2. Processamento (View e Tabela Mensal)
        rodar_processamento()
        
        # 3. Inteligência
        rodar_narracao()
        
        # 4. Distribuição
        # rodar_exportacao()  <-- COMENTADO (ou apagado)
        
        print("✅ Pipeline executada com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()