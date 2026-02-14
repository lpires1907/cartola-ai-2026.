import pandas as pd
from google.cloud import bigquery
import os

def recriar_view_consolidada(client, dataset_id):
    """
    Recria a View Consolidada garantindo todas as colunas necessárias para o App.
    """
    view_id = f"{client.project}.{dataset_id}.view_consolidada_times"
    tab_historico = f"{client.project}.{dataset_id}.historico"
    
    print(f"🔨 (Re)Construindo View Consolidada Blindada: {view_id}")

    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH Unificado AS (
        -- Deduplicação: Prioriza OFICIAL sobre PARCIAL
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
        MIN(pontos) as menor_pontuacao,
        COUNT(DISTINCT rodada) as rodadas_jogadas,
        MAX(patrimonio) as patrimonio_atual,
        
        -- Turnos
        SUM(CASE WHEN rodada <= 19 THEN pontos ELSE 0 END) as pontos_turno_1,
        SUM(CASE WHEN rodada > 19 THEN pontos ELSE 0 END) as pontos_turno_2,
        
        -- Meses
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
        print("✅ View Consolidada atualizada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao criar View: {e}")

def atualizar_campeoes_mensais(client, dataset_id):
    """
    Atualiza a tabela de campeões mensais (Função que estava faltando).
    """
    print("📊 Atualizando Tabela de Campeões Mensais...")
    tab_historico = f"{client.project}.{dataset_id}.historico"
    tab_mensal = f"{client.project}.{dataset_id}.Rodada_Mensal"

    # Query segura com MERGE para atualizar ou inserir novos meses
    query_merge = f"""
    MERGE `{tab_mensal}` T
    USING (
        WITH PontosPorMes AS (
            SELECT m.Mensal, h.nome, SUM(h.pontos) as pts
            FROM `{tab_historico}` h
            JOIN `{tab_mensal}` m ON h.rodada = m.Rodada
            GROUP BY 1, 2
        ),
        Ranking AS (
            SELECT Mensal, nome, pts, ROW_NUMBER() OVER(PARTITION BY Mensal ORDER BY pts DESC) as pos
            FROM PontosPorMes
        ),
        Vencedores AS (
            SELECT Mensal, MAX(CASE WHEN pos = 1 THEN nome END) as campeao, MAX(CASE WHEN pos = 2 THEN nome END) as vice
            FROM Ranking WHERE pos <= 2 GROUP BY 1
        )
        SELECT m.Rodada, v.campeao, v.vice, CAST(CURRENT_TIMESTAMP() AS STRING) as data_up
        FROM `{tab_mensal}` m
        LEFT JOIN Vencedores v ON m.Mensal = v.Mensal
    ) S
    ON T.Rodada = S.Rodada
    WHEN MATCHED THEN
        UPDATE SET `Campeao ` = S.campeao, Vice = S.vice, DataStatus = S.data_up
    """ # nosec B608
    
    try:
        client.query(query_merge).result()
        print("✅ Tabela Mensal de Campeões atualizada!")
    except Exception as e:
        print(f"⚠️ Aviso ao atualizar mensais (Tabela pode não existir ainda): {e}")

if __name__ == "__main__":
    pass
