def recriar_view_consolidada(client, dataset_id):
    view_id = f"{client.project}.{dataset_id}.view_consolidada_times"
    tab_h = f"{client.project}.{dataset_id}.historico"
    
    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    WITH Unificado AS (
        SELECT * EXCEPT(rn) FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY nome, rodada ORDER BY CASE WHEN tipo_dado = 'OFICIAL' THEN 1 ELSE 2 END, timestamp DESC) as rn
            FROM `{tab_h}`
        ) WHERE rn = 1
    )
    SELECT 
        nome, MAX(nome_cartola) as nome_cartola, SUM(pontos) as total_geral,
        AVG(pontos) as media, MAX(pontos) as maior_pontuacao,
        COUNT(DISTINCT rodada) as rodadas_jogadas, MAX(patrimonio) as patrimonio_atual
    FROM Unificado
    GROUP BY nome
    """ # nosec B608
    client.query(query).result()
    print("✅ View Consolidada Restaurada.")
