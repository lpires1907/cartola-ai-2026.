import json
import base64
import os

KEY_FILE = r"C:\Users\Lenovo\Downloads\cartolafc-485703-3eb0ff9ca7cd.json"
OUTPUT_TOML = r"C:\Users\Lenovo\cartolaai2026\streamlit_secret.toml"
OUTPUT_ENV = r"C:\Users\Lenovo\cartolaai2026\.env"

# --- Load clean key ---
with open(KEY_FILE, "r", encoding="utf-8") as f:
    info = json.load(f)

print(f"[OK] Key loaded OK")
print(f"   Project : {info.get('project_id')}")
print()

# --- Build TOML block (Format: Single line JSON string for GCP_SERVICE_ACCOUNT) ---
# This is the most reliable way to avoid multiline issues in Streamlit TOML
clean_json = json.dumps(info)
toml_content = f'GCP_SERVICE_ACCOUNT = {json.dumps(clean_json)}'

with open(OUTPUT_TOML, "w", encoding="utf-8") as f:
    f.write(toml_content)

print("=== CONTEUDO PARA STREAMLIT SECRETS ===")
print("Copie TUDO abaixo e cole no painel do Streamlit Cloud:")
print("-" * 30)
print(toml_content)
print("-" * 30)
print()

# --- Also update .env with clean single-line JSON ---
token_line = ""
gemini_line = ""

try:
    if os.path.exists(OUTPUT_ENV):
        with open(OUTPUT_ENV, "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("CARTOLA_GLBID="):
                    token_line = line.strip()
                elif line.startswith("GEMINI_API_KEY="):
                    gemini_line = line.strip()
except Exception:
    pass

env_content = (
    "# Local development secrets - DO NOT COMMIT\n"
    f"GCP_SERVICE_ACCOUNT={json.dumps(info)}\n"
    f"{token_line}\n"
    f"{gemini_line}\n"
)

with open(OUTPUT_ENV, "w", encoding="utf-8") as f:
    f.write(env_content)

print(f"[OK] .env atualizado.")
