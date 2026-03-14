import os
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# === Diretórios ===
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"

# Garantir que os diretórios existam
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# === Portal da Transparência ===
TRANSPARENCIA_PAGE_URL = "https://portaldatransparencia.gov.br/download-de-dados/servidores"
CDN_BASE_URL = "https://dadosabertos-download.cgu.gov.br/PortalDaTransparencia/saida/servidores"

ANO_INICIO = 2020

# === SharePoint ===
SHAREPOINT_SITE_URL = os.getenv("SHAREPOINT_SITE_URL", "")
SHAREPOINT_CLIENT_ID = os.getenv("SHAREPOINT_CLIENT_ID", "")
SHAREPOINT_CLIENT_SECRET = os.getenv("SHAREPOINT_CLIENT_SECRET", "")
SHAREPOINT_TENANT_ID = os.getenv("SHAREPOINT_TENANT_ID", "")
SHAREPOINT_FOLDER_PATH = os.getenv("SHAREPOINT_FOLDER_PATH", "transparencia_servidores_executivo_federal")

# === Configurações de download ===
DOWNLOAD_TIMEOUT = 300  # segundos
DOWNLOAD_CHUNK_SIZE = 8192  # bytes
MAX_RETRIES = 3

# Upload chunked: arquivos acima deste tamanho usam upload por sessão
CHUNK_UPLOAD_THRESHOLD = 4 * 1024 * 1024  # 4 MB

# === Arquivo de rastreamento de downloads concluídos ===
DOWNLOADS_TRACKER_FILE = DATA_DIR / "downloads_concluidos.json"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "https://portaldatransparencia.gov.br/",
    "Origin": "https://portaldatransparencia.gov.br",
}


def carregar_downloads_concluidos() -> dict:
    """Carrega o registro de downloads já concluídos."""
    if DOWNLOADS_TRACKER_FILE.exists():
        return json.loads(DOWNLOADS_TRACKER_FILE.read_text(encoding="utf-8"))
    return {}


def salvar_downloads_concluidos(dados: dict):
    """Salva o registro de downloads concluídos."""
    DOWNLOADS_TRACKER_FILE.write_text(
        json.dumps(dados, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
