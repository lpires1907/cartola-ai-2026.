import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES ---
GEMINI_KEY = os.getenv('GEMINI_API_KEY')
GCP_JSON = os.getenv('GCP_SERVICE_ACCOUNT')
DATASET_ID = "cartola_analytics"
TAB_HISTORICO = f"{DATASET_ID}.historico"
TAB_CORNETA = f"{DATASET_ID}.comentarios_ia"
VIEW_CONSOLIDADA = f"{DATASET_ID}.view_consolidada_times"

def get_bq_client():
    info = json.loads(GCP_JSON)
    creds = service_account.Credentials.from_service_account_info(info)
    return bigquery.Client(credentials=creds, project=info['project_id'])

# --- CHECAGENS DE REDUNDÂNCIA ---
def ja_comentou(client, rodada, tipo):
    """Verifica se já existe comentário desse TIPO para esta rodada"""
    try:
        # Verifica se a coluna 'tipo' existe antes de filtrar
        schema = client.get_table(f"{client.project}.{TAB_CORNETA}").schema
        tem_coluna_tipo = any(field.name == 'tipo' for field in schema)
        
        if not tem_coluna_tipo: return False # Se não tem coluna, força gerar novo para atualizar schema

        query = f"""
            SELECT COUNT(*) as qtd FROM `{client.project}.{TAB_CORNETA}` 
            WHERE rodada = {rodada} AND tipo = '{tipo}'
        """
        res = list(client.query(query).result())
        return res[0].qtd > 0
    except: return False

# --- FUNÇÕES DE GERAÇÃO (IA) ---
def chamar_gemini(prompt):
    if not GEMINI_KEY: return "IA sem contrato."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
    try:
        res = requests.post(url, headers={'Content-Type': 'application/json'}, json={"contents": [{"parts": [{"text": prompt}]}]})
        if res.status_code == 200:
            return res.json()['candidates'][0]['content']['parts'][0]['text']
    except Exception as e:
        print(f"Erro Gemini: {e}")
    return None

def gerar_analise_rodada(df_ranking, rodada, status_rodada):
    lider = df_ranking.iloc[0]
    lanterna = df_ranking.iloc[-1]
    vice = df_ranking.iloc[1] if len(df_ranking) > 1 else None
    
    txt_status = "AO VIVO" if status_rodada == 'PARCIAL' else "FINALIZADA"
    
    prompt = f"""
    Atue como um narrador de futebol sarcástico. Rodada {rodada} ({txt_status}).
    - Destaque: {lider['nome']} ({lider['pontos']} pts).
    - Zueira: {lanterna['nome']} ({lanterna['pontos']} pts).
    {f"- Alerta: {vice['nome']} está na cola!" if vice is not None else ""}
    Faça um comentário curto (max 280 chars) e engraçado.
    """
    return chamar_gemini(prompt)

def gerar_analise_geral(df_view, rodada_atual):
    # Pega dados estatísticos da View
    lider_geral = df_view.iloc[0]
    maior_media = df_view.sort_values('media_pontos', ascending=False).iloc[0]
    maior_pico = df_view.sort_values('maior_pontuacao', ascending=False).iloc[0]
    regular = df_view.sort_values('mediana_pontos', ascending=False).iloc[0]
    
    total_rodadas = 38
    rodadas_restantes = total_rodadas - rodada_atual
    
    prompt = f"""
    Atue como um Analista de Dados Esportivos sério, mas com pitadas de humor.
    Analise o CAMPEONATO GERAL até a Rodada {rodada_atual} de {total_rodadas}.
    
    DADOS CONSOLIDADOS:
    1. Líder Geral: {lider_geral['nome']} (Total: {lider_geral['total_geral']:.1f}).
    2. Maior Média: {maior_media['nome']} ({maior_media['media_pontos']:.1f}/rodada).
    3. Time mais Regular (Mediana): {regular['nome']}.
    4. Maior "Mitada" (Máx): {maior_pico['nome']} ({maior_pico['maior_pontuacao']:.1f} pts numa só rodada).
    
    CONTEXTO:
    Faltam {rodadas_restantes} rodadas.
    
    MISSÃO:
    Escreva um parágrafo de análise (max 500 chars).
    Compare a constância (média/mediana) com a sorte (pico).
    Diga se o campeonato está aberto ou se o líder está disparando.
    """
    return chamar_gemini(prompt)

# --- MAIN ---
def main():
    if not GCP_JSON: print("Erro: Sem credenciais GCP"); return
    client = get_bq_client()
    ts_agora = datetime.now(pytz.timezone('America/Sao_Paulo'))

    # 1. Obter Rodada Atual e Status
    try:
        query_meta = f"SELECT rodada, tipo_dado FROM `{client.project}.{TAB_HISTORICO}` ORDER BY timestamp DESC LIMIT 1"
        df_meta = client.query(query_meta).to_dataframe()
        if df_meta.empty: print("Sem dados."); return
        
        rodada_atual = int(df_meta['rodada'].iloc[0])
        status_dados = df_meta['tipo_dado'].iloc[0]
    except: print("Erro ao ler metadados."); return

    print(f"🎤 Iniciando Narrador | Rodada {rodada_atual} ({status_dados})")

    # --- BLOCO 1: COMENTÁRIO DA RODADA (MICRO) ---
    if not ja_comentou(client, rodada_atual, 'RODADA'):
        print("⚡ Gerando análise da RODADA...")
        query_round = f"SELECT * FROM `{client.project}.{TAB_HISTORICO}` WHERE rodada = {rodada_atual} ORDER BY pontos DESC"
        df_round = client.query(query_round).to_dataframe()
        
        texto_rodada = gerar_analise_rodada(df_round, rodada_atual, status_dados)
        
        if texto_rodada:
            salvar_comentario(client, texto_rodada, rodada_atual, 'RODADA', ts_agora)
    else:
        print("zzz Análise de RODADA já feita.")

    # --- BLOCO 2: COMENTÁRIO GERAL (MACRO) ---
    # Só gera análise geral se for dados OFICIAIS (fechados), para não oscilar com parciais
    if status_dados == 'OFICIAL' and not ja_comentou(client, rodada_atual, 'GERAL'):
        print("🧠 Gerando análise GERAL (Estatística)...")
        query_view = f"SELECT * FROM `{client.project}.{VIEW_CONSOLIDADA}` ORDER BY total_geral DESC"
        df_view = client.query(query_view).to_dataframe()
        
        texto_geral = gerar_analise_geral(df_view, rodada_atual)
        
        if texto_geral:
            salvar_comentario(client, texto_geral, rodada_atual, 'GERAL', ts_agora)
    else:
        print("zzz Análise GERAL já feita ou dados ainda são parciais.")

def salvar_comentario(client, texto, rodada, tipo, ts):
    df = pd.DataFrame([{
        'texto': texto,
        'rodada': rodada,
        'tipo': tipo,  # <--- NOVA COLUNA
        'data': ts
    }])
    
    # Schema com a nova coluna 'tipo'
    schema = [
        bigquery.SchemaField("texto", "STRING"),
        bigquery.SchemaField("rodada", "INTEGER"),
        bigquery.SchemaField("tipo", "STRING"), # <--- Adicionado
        bigquery.SchemaField("data", "TIMESTAMP")
    ]
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        schema=schema
    )
    
    try:
        client.load_table_from_dataframe(df, f"{client.project}.{TAB_CORNETA}", job_config=job_config).result()
        print(f"💾 Comentário ({tipo}) salvo!")
    except Exception as e:
        print(f"❌ Erro ao salvar: {e}")

if __name__ == "__main__":
    main()