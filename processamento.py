import pandas as pd
from google.cloud import bigquery
import os

def recriar_view_consolidada(client, dataset_id):
    view_id = f"{client.project}.{dataset_id}.view_consolidada_times"
    tab_historico = f"{client.project}.{dataset_id}.historico"
    
    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    SELECT 
        nome, MAX(nome_cartola) as nome_cartola, SUM(pontos) as total_geral,
        AVG(pontos) as media, MAX(pontos) as maior_pontuacao,
        COUNT(DISTINCT rodada) as rodadas_jogadas, MAX(patrimonio) as patrimonio_atual
    FROM `{tab_historico}`
    GROUP BY nome
    """ # nosec B608
    client.query(query).result()
    print("✅ View Consolidada da Liga SAS restaurada!")

if __name__ == "__main__":
    pass
