import config
import json

client = config.get_bq_client()
if not client:
    print("ERRO: Não foi possível conectar ao BigQuery via config.py")
    exit(1)

print(f"Listando tabelas em {client.project}.cartola_analytics:")
try:
    dataset_ref = client.dataset("cartola_analytics")
    tables = client.list_tables(dataset_ref)
    found = False
    for table in tables:
        found = True
        t_obj = client.get_table(table.reference)
        print(f"- {table.table_id:25} | {t_obj.num_rows:6} linhas | {table.table_type}")
    if not found:
        print("Nenhuma tabela encontrada no dataset.")
except Exception as e:
    print(f"ERRO: {e}")
