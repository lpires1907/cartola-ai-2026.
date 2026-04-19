import sys
import os

# Adiciona a pasta src ao path para localizar os módulos movidos
sys.path.append(os.path.join(os.getcwd(), 'src'))

# Importa e executa a função principal do pipeline
from pipeline import main

if __name__ == "__main__":
    main()
