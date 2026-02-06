import sys
from coletor import rodar_coleta
from processamento import rodar_processamento # <--- NOVO IMPORT
from narrador import rodar_narracao
from exportador import rodar_exportacao

def main():
    print("🚀 INICIANDO PIPELINE CARTOLA ANALYTICS")
    
    try:
        # 1. ETL
        rodar_coleta()
        
        # 2. Processamento (View e Tabela Mensal)
        # Importante rodar aqui para que o Narrador e o App já peguem os campeões mensais atualizados
        rodar_processamento()
        
        # 3. Inteligência
        rodar_narracao()
        
        # 4. Distribuição
        rodar_exportacao()
        
        print("✅ Pipeline executada com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro crítico: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()