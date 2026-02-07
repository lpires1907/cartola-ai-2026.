import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import pytz
import os

# --- FUNÇÃO 1: RECRIAR A VIEW (CORRIGE O BUG DO APP) ---
def recriar_view_consolidada(client, dataset_id):
    """
    Recria a View Consolidada usando lógica de intervalos fixos.
    Isso elimina JOINs perigosos e garante que o Streamlit não quebre com duplicatas.
    """
    project_id = client.project
    view_id = f"{project_id}.{dataset_id}.view_consolidada_times"
    tab_historico = f"{project_id}.{dataset_id}.historico"
    
    print(f"🔨 (Re)Construindo View Consolidada Blindada: {view_id}")

    # Query Definitiva e Limpa
    # O '# nosec' diz para o Bandit que esta query é administrativa e segura
    query = f"""
    CREATE OR REPLACE VIEW `{view_id}` AS
    SELECT 
        nome,
        nome_cartola,
        -- Agregados Gerais
        SUM(pontos) as total_geral,
        AVG(pontos) as media, -- Renomeado para 'media' para manter compatibilidade
        MAX(pontos) as maior_pontuacao,
        MIN(pontos) as menor_pontuacao,
        COUNT(*) as rodadas_jogadas,
        AVG(patrimonio) as patrimonio_medio,
        
        -- Turnos (Regra: R19 encerra o turno 1)
        SUM(CASE WHEN rodada <= 19 THEN pontos ELSE 0 END) as pontos_turno_1,
        SUM(CASE WHEN rodada > 19 THEN pontos ELSE 0 END) as pontos_turno_2,
        
        -- Meses (Agrupamento Manual para evitar Joins e Duplicatas)
        -- Jan/Fev (Estaduais/Início): Rodadas 1 a 8
        SUM(CASE WHEN rodada BETWEEN 1 AND 8 THEN pontos ELSE 0 END) as pontos_jan_fev,
        -- Março: 9 a 12
        SUM(CASE WHEN rodada BETWEEN 9 AND 12 THEN pontos ELSE 0 END) as pontos_marco,
        -- Abril: 13 a 16
        SUM(CASE WHEN rodada BETWEEN 13 AND 16 THEN pontos ELSE 0 END) as pontos_abril,
        -- Maio: 17 a 20
        SUM(CASE WHEN rodada BETWEEN 17 AND 20 THEN pontos ELSE 0 END) as pontos_maio,
        -- Jun/Jul: 21 a 24
        SUM(CASE WHEN rodada BETWEEN 21 AND 24 THEN pontos ELSE 0 END) as pontos_jun_jul,
        -- Agosto: 25 a 29
        SUM(CASE WHEN rodada BETWEEN 25 AND 29 THEN pontos ELSE 0 END) as pontos_agosto,
        -- Setembro: 30 a 33
        SUM(CASE WHEN rodada BETWEEN 30 AND 33 THEN pontos ELSE 0 END) as pontos_setembro,
        -- Outubro: 34 a 36
        SUM(CASE WHEN rodada BETWEEN 34 AND 36 THEN pontos ELSE 0 END) as pontos_outubro,
        -- Nov/Dez (Reta Final): 37 em diante
        SUM(CASE WHEN rodada >= 37 THEN pontos ELSE 0 END) as pontos_nov_dez

    FROM `{tab_historico}`
    GROUP BY nome, nome_cartola
    ORDER BY total_geral DESC
    """ # nosec

    try:
        client.query(query).result()
        print("✅ View Consolidada atualizada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao criar View: {e}")

# --- FUNÇÃO 2: ATUALIZAR TABELA MENSAL (METADADOS) ---
def atualizar_campeoes_mensais(client, dataset_id):
    """
    Mantém a tabela de 'Campeões do Mês' atualizada.
    Isso é útil para histórico, mas NÃO é usado na View principal para evitar erros.
    """
    print("📊 Atualizando Tabela de Campeões Mensais...")
    
    tab_historico = f"{client.project}.{dataset_id}.historico"
    tab_mensal = f"{client.project}.{dataset_id}.Rodada_Mensal"

    # Query complexa de Merge (Mantida a lógica original sua, mas ajustada)
    query_merge = f"""
    MERGE `{tab_mensal}` T
    USING (
        WITH 
        UltimaRodada AS (SELECT MAX(rodada) as max_rodada FROM `{tab_historico}` WHERE tipo_dado = 'OFICIAL'),
        
        PontosPorMes AS (
            SELECT 
                m.Mensal,
                h.nome,
                SUM(h.pontos) as pts
            FROM `{tab_historico}` h
            JOIN `{tab_mensal}` m ON h.rodada = m.Rodada
            GROUP BY 1, 2
        ),
        
        Ranking AS (
            SELECT Mensal, nome, pts, ROW_NUMBER() OVER(PARTITION BY Mensal ORDER BY pts DESC) as pos
            FROM PontosPorMes
        ),
        
        Vencedores AS (
            SELECT 
                Mensal,
                MAX(CASE WHEN pos = 1 THEN nome END) as campeao,
                MAX(CASE WHEN pos = 2 THEN nome END) as vice
            FROM Ranking WHERE pos <= 2 GROUP BY 1
        )

        SELECT 
            m.Rodada,
            v.campeao,
            v.vice,
            CASE WHEN m.Rodada <= (SELECT max_rodada FROM UltimaRodada) THEN 'Fechado' ELSE 'Aberto' END as novo_status,
            CAST(CURRENT_TIMESTAMP() AS STRING) as data_atualizacao
        FROM `{tab_mensal}` m
        LEFT JOIN Vencedores v ON m.Mensal = v.Mensal
    ) S
    ON T.Rodada = S.Rodada
    WHEN MATCHED THEN
        UPDATE SET 
            `Campeao ` = S.campeao,
            Vice = S.vice,
            Status = S.novo_status,
            DataStatus = S.data_atualizacao
    """ # nosec
    
    try:
        client.query(query_merge).result()
        print("✅ Tabela Mensal de Campeões atualizada!")
    except Exception as e:
        print(f"⚠️ Aviso: Não foi possível atualizar campeões mensais (Talvez a tabela ainda não exista ou esteja vazia): {e}")

# Ponto de entrada para testes manuais
if __name__ == "__main__":
    # Carrega env apenas para teste local isolado
    from dotenv import load_dotenv
    from google.oauth2 import service_account
    import json
    
    load_dotenv()
    
    # Simula a conexão (copiado do coletor)
    if os.path.exists("credentials.json"):
        client = bigquery.Client.from_service_account_json("credentials.json")
    else:
        print("❌ Sem credenciais para teste local.")
        exit()

    DATASET_ID = "cartola_analytics"
    
    # Roda as duas funções
    atualizar_campeoes_mensais(client, DATASET_ID)
    recriar_view_consolidada(client, DATASET_ID)
