import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES ---
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_MENSAL = f"{DATASET_ID}.Rodada_Mensal"
VIEW_CONSOLIDADA = f"{DATASET_ID}.view_consolidada_times"

def get_bq_client():
    if os.getenv('GCP_SERVICE_ACCOUNT'):
        info = json.loads(os.getenv('GCP_SERVICE_ACCOUNT'))
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(credentials=creds, project=info['project_id'])
    return None

def atualizar_campeoes_mensais():
    client = get_bq_client()
    if not client: return

    print("📊 Atualizando Campeões Mensais e Status...")

    # Query poderosa que:
    # 1. Agrupa pontos por Mês e Time
    # 2. Rankeia (1º e 2º)
    # 3. Atualiza a tabela Rodada_Mensal com o Líder, Vice e Status
    
    query_merge = f"""
    MERGE `{client.project}.{TAB_MENSAL}` T
    USING (
        WITH 
        UltimaRodada AS (SELECT MAX(rodada) as max_rodada FROM `{client.project}.{TAB_HISTORICO}` WHERE tipo_dado = 'OFICIAL'),
        
        PontosPorMes AS (
            SELECT 
                m.Mensal,
                h.nome,
                SUM(h.pontos) as pts
            FROM `{client.project}.{TAB_HISTORICO}` h
            JOIN `{client.project}.{TAB_MENSAL}` m ON h.rodada = m.Rodada
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
            CURRENT_TIMESTAMP() as data_atualizacao
        FROM `{client.project}.{TAB_MENSAL}` m
        LEFT JOIN Vencedores v ON m.Mensal = v.Mensal
    ) S
    ON T.Rodada = S.Rodada
    WHEN MATCHED THEN
        UPDATE SET 
            Campeao = S.campeao,
            Vice = S.vice,
            Status = S.novo_status,
            DataStatus = S.data_atualizacao
    """
    
    try:
        client.query(query_merge).result()
        print("✅ Tabela Mensal atualizada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao atualizar mensal: {e}")

def criar_view_completa():
    client = get_bq_client()
    if not client: return

    print("🔨 Recriando View Consolidada...")

    # Colunas dinâmicas para cada mês do seu CSV
    cols_mensais = """
        SUM(CASE WHEN m.Mensal = 'Jan Fev' THEN h.pontos ELSE 0 END) as pontos_jan_fev,
        SUM(CASE WHEN m.Mensal = 'Março' THEN h.pontos ELSE 0 END) as pontos_marco,
        SUM(CASE WHEN m.Mensal = 'Abril' THEN h.pontos ELSE 0 END) as pontos_abril,
        SUM(CASE WHEN m.Mensal = 'Maio' THEN h.pontos ELSE 0 END) as pontos_maio,
        SUM(CASE WHEN m.Mensal = 'Jun Jul' THEN h.pontos ELSE 0 END) as pontos_jun_jul,
        SUM(CASE WHEN m.Mensal = 'Agosto' THEN h.pontos ELSE 0 END) as pontos_agosto,
        SUM(CASE WHEN m.Mensal = 'Setembro' THEN h.pontos ELSE 0 END) as pontos_setembro,
        SUM(CASE WHEN m.Mensal = 'Outubro' THEN h.pontos ELSE 0 END) as pontos_outubro,
        SUM(CASE WHEN m.Mensal = 'Nov Dez' THEN h.pontos ELSE 0 END) as pontos_nov_dez
    """

    sql = f"""
    CREATE OR REPLACE VIEW `{client.project}.{VIEW_CONSOLIDADA}` AS
    SELECT 
        h.nome,
        h.nome_cartola,
        
        -- GERAL
        SUM(h.pontos) as total_geral,
        AVG(h.pontos) as media,
        MAX(h.pontos) as maior_pontuacao,
        MIN(h.pontos) as menor_pontuacao,
        
        -- TURNOS (1º Turno: R1-R19 | 2º Turno: R20-R38)
        SUM(CASE WHEN h.rodada <= 19 THEN h.pontos ELSE 0 END) as pontos_turno_1,
        SUM(CASE WHEN h.rodada >= 20 THEN h.pontos ELSE 0 END) as pontos_turno_2,
        
        -- MENSAIS
        {cols_mensais}

    FROM `{client.project}.{TAB_HISTORICO}` h
    LEFT JOIN `{client.project}.{TAB_MENSAL}` m ON h.rodada = m.Rodada
    GROUP BY h.nome, h.nome_cartola
    ORDER BY total_geral DESC;
    """
    
    try:
        client.query(sql).result()
        print("✅ View Consolidada criada!")
    except Exception as e:
        print(f"❌ Erro ao criar view: {e}")

def rodar_processamento():
    atualizar_campeoes_mensais()
    criar_view_completa()

if __name__ == "__main__":
    rodar_processamento()