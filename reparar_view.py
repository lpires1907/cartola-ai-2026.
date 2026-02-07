import os
from google.cloud import bigquery
from dotenv import load_dotenv

# Carrega credenciais
load_dotenv()
client = bigquery.Client()

DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{client.project}.{DATASET_ID}.historico"
VIEW_CONSOLIDADA = f"{client.project}.{DATASET_ID}.view_consolidada_times"

print(f"🔧 Iniciando reparo na View: {VIEW_CONSOLIDADA}")

# Query Definitiva e Limpa (Sem SELECT * para evitar duplicatas)
query_view = f"""
CREATE OR REPLACE VIEW `{VIEW_CONSOLIDADA}` AS
SELECT 
    nome,
    nome_cartola,
    -- Agregados Gerais
    SUM(pontos) as total_geral,
    AVG(pontos) as media_pontos,
    MAX(pontos) as maior_pontuacao,
    MIN(pontos) as menor_pontuacao,
    COUNT(*) as rodadas_jogadas,
    AVG(patrimonio) as patrimonio_medio,
    
    -- Turnos
    SUM(CASE WHEN rodada <= 19 THEN pontos ELSE 0 END) as pontos_turno_1,
    SUM(CASE WHEN rodada > 19 THEN pontos ELSE 0 END) as pontos_turno_2,
    
    -- Meses (Agrupamento Manual para evitar Joins complexos)
    -- Ajuste conforme calendário 2026 real, aqui é uma estimativa robusta
    SUM(CASE WHEN rodada BETWEEN 1 AND 8 THEN pontos ELSE 0 END) as pontos_jan_fev, -- Estaduais
    SUM(CASE WHEN rodada BETWEEN 9 AND 12 THEN pontos ELSE 0 END) as pontos_marco,
    SUM(CASE WHEN rodada BETWEEN 13 AND 16 THEN pontos ELSE 0 END) as pontos_abril,
    SUM(CASE WHEN rodada BETWEEN 17 AND 20 THEN pontos ELSE 0 END) as pontos_maio,
    SUM(CASE WHEN rodada BETWEEN 21 AND 24 THEN pontos ELSE 0 END) as pontos_jun_jul,
    SUM(CASE WHEN rodada BETWEEN 25 AND 29 THEN pontos ELSE 0 END) as pontos_agosto,
    SUM(CASE WHEN rodada BETWEEN 30 AND 33 THEN pontos ELSE 0 END) as pontos_setembro,
    SUM(CASE WHEN rodada BETWEEN 34 AND 36 THEN pontos ELSE 0 END) as pontos_outubro,
    SUM(CASE WHEN rodada >= 37 THEN pontos ELSE 0 END) as pontos_nov_dez

FROM `{TAB_HISTORICO}`
GROUP BY nome, nome_cartola
"""

try:
    print("⏳ Recriando a View no BigQuery...")
    client.query(query_view).result()
    print("✅ SUCESSO! A View foi regenerada e limpa.")
    print("👉 Agora os dados numéricos (total_geral) são garantidamente únicos.")
except Exception as e:
    print(f"❌ Erro ao recriar view: {e}")
