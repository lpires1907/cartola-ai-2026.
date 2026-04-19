#!/usr/bin/env python3
import os
import sys
from pathlib import Path

def validate_project(repo_path):
    path = Path(repo_path).resolve()
    if not path.exists() or not path.is_dir():
        print(f"Erro: O diretorio '{repo_path}' nao foi encontrado.")
        sys.exit(1)

    # Regras de validacao
    essential_files = ['README.md', '.gitignore', 'LICENSE', 'CONTRIBUTING.md']
    expected_dirs = [
        (['src', 'lib'], "codigo-fonte principal"),
        (['tests', 'spec'], "testes automatizados"),
        (['docs'], "documentacao complementar"),
        (['.github/workflows'], "pipeline de CI/CD")
    ]

    missing_files = [f for f in essential_files if not (path / f).is_file()]
    
    missing_dirs = []
    for dir_options, desc in expected_dirs:
        found = any((path / d).is_dir() for d in dir_options)
        if not found:
            missing_dirs.append(f"{dir_options}/ (para {desc})")

    # Avaliacao do resultado
    if missing_files or missing_dirs:
        print("[ALERTA] AUDITORIA FALHOU: A estrutura atual nao atende aos padroes minimos.\n")
        
        if missing_files:
            print("Arquivos essenciais ausentes:")
            for f in missing_files:
                print(f"  - {f}")
                
        if missing_dirs:
            print("\nDiretorios estruturais ausentes:")
            for d in missing_dirs:
                print(f"  - {d}")
                
        print("\nINSTRUCAO PARA O AGENTE: Proponha a criacao das pastas e os templates basicos para os arquivos ausentes listados acima.")
        sys.exit(1)
    else:
        print("[OK] SUCESSO: O projeto atende a todos os requisitos de arquitetura e documentacao basica.")
        sys.exit(0)

if __name__ == '__main__':
    target = sys.argv[1] if len(sys.argv) > 1 else '.'
    validate_project(target)
