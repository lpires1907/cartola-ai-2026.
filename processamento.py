import pandas as pd
from google.cloud import bigquery
import os

def recriar_view_consolidada(client, dataset_id):
    view_id = f"{client.project}.{dataset_id}.view_consolidada_times"
    tab_historico = f"{client.project}.{dataset_id}.historico"
    
    print(f"🔨 (Re)Construindo View Completa: {view_id}")

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH Unificado AS (
        SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER(
                PARTITION BY nome, rodada 
                ORDER BY CASE WHEN tipo_dado = 'OFICIAL' THEN 1 ELSE 2 END, timestamp DESC
            ) as rn
            FROM `{tab_historico}`
        ) WHERE rn = 1
    )
    SELECT 
        nome,
        MAX(nome_cartola) as nome_cartola,
        SUM(pontos) as total_geral,
        AVG(pontos) as media,
        MAX(pontos) as maior_pontuacao,
        -- Zicada: Mínimo acima de zero
        MIN(CASE WHEN pontos > 0 THEN pontos ELSE NULL END) as menor_pontuacao,
        COUNT(DISTINCT rodada) as rodadas_jogadas,
        MAX(patrimonio) as patrimonio_atual,
        
        -- Turnos
        SUM(CASE WHEN rodada <= 19 THEN pontos ELSE 0 END) as pontos_turno_1,
        SUM(CASE WHEN rodada > 19 THEN pontos ELSE 0 END) as pontos_turno_2,
        
        -- Meses (Jan-Dez)
        SUM(CASE WHEN rodada BETWEEN 1 AND 8 THEN pontos ELSE 0 END) as pontos_jan_fev,
        SUM(CASE WHEN rodada BETWEEN 9 AND 12 THEN pontos ELSE 0 END) as pontos_marco,
        SUM(CASE WHEN rodada BETWEEN 13 AND 16 THEN pontos ELSE 0 END) as pontos_abril,
        SUM(CASE WHEN rodada BETWEEN 17 AND 20 THEN pontos ELSE 0 END) as pontos_maio,
        SUM(CASE WHEN rodada BETWEEN 21 AND 24 THEN pontos ELSE 0 END) as pontos_jun_jul,
        SUM(CASE WHEN rodada BETWEEN 25 AND 29 THEN pontos ELSE 0 END) as pontos_agosto,
        SUM(CASE WHEN rodada BETWEEN 30 AND 33 THEN pontos ELSE 0 END) as pontos_setembro,
        SUM(CASE WHEN rodada BETWEEN 34 AND 36 THEN pontos ELSE 0 END) as pontos_outubro,
        SUM(CASE WHEN rodada >= 37 THEN pontos ELSE 0 END) as pontos_nov_dez

    FROM Unificado
    GROUP BY nome
    ORDER BY total_geral DESC
    """ # nosec B608

    try:
        client.query(query).result()
        print("✅ View Consolidada atualizada!")
    except Exception as e:
        print(f"❌ Erro View: {e}")

def atualizar_campeoes_mensais(client, dataset_id):
    pass

if __name__ == "__main__":
    pass
