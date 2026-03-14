import logging
from pathlib import Path

import msal
import requests

from src.config import (
    SHAREPOINT_SITE_URL,
    SHAREPOINT_CLIENT_ID,
    SHAREPOINT_CLIENT_SECRET,
    SHAREPOINT_TENANT_ID,
    SHAREPOINT_FOLDER_PATH,
    DATA_DIR,
    CHUNK_UPLOAD_THRESHOLD,
)

logger = logging.getLogger("transparencia_etl")

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


def _validar_credenciais():
    """Valida que todas as credenciais SharePoint foram configuradas."""
    campos = {
        "SHAREPOINT_SITE_URL": SHAREPOINT_SITE_URL,
        "SHAREPOINT_CLIENT_ID": SHAREPOINT_CLIENT_ID,
        "SHAREPOINT_CLIENT_SECRET": SHAREPOINT_CLIENT_SECRET,
        "SHAREPOINT_TENANT_ID": SHAREPOINT_TENANT_ID,
    }
    faltando = [k for k, v in campos.items() if not v or v.startswith("seu_")]
    if faltando:
        raise ValueError(
            f"Credenciais SharePoint não configuradas no .env: {', '.join(faltando)}. "
            "Consulte o .env.example para instruções."
        )


def _criar_msal_app() -> msal.ConfidentialClientApplication:
    """Cria instância MSAL para obtenção/renovação de tokens."""
    _validar_credenciais()
    authority = f"https://login.microsoftonline.com/{SHAREPOINT_TENANT_ID}"
    return msal.ConfidentialClientApplication(
        SHAREPOINT_CLIENT_ID,
        authority=authority,
        client_credential=SHAREPOINT_CLIENT_SECRET,
    )


def _obter_token(app: msal.ConfidentialClientApplication) -> str:
    """Obtém access token via MSAL client credentials flow."""
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])

    if "access_token" not in result:
        erro = result.get("error_description", result.get("error", "Erro desconhecido"))
        raise ValueError(f"Falha ao obter token: {erro}")

    return result["access_token"]


def _headers(token: str) -> dict:
    """Headers padrão para chamadas ao Microsoft Graph."""
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def _extrair_host_e_path(site_url: str) -> tuple[str, str]:
    """Extrai hostname e site path da URL do SharePoint.
    Ex: 'https://xploredados.sharepoint.com/sites/Produto-BI' -> ('xploredados.sharepoint.com', '/sites/Produto-BI')
    """
    from urllib.parse import urlparse
    parsed = urlparse(site_url)
    return parsed.hostname, parsed.path.rstrip("/")


def _obter_site_id(token: str) -> str:
    """Obtém o site ID do SharePoint via Microsoft Graph."""
    hostname, site_path = _extrair_host_e_path(SHAREPOINT_SITE_URL)
    url = f"{GRAPH_BASE}/sites/{hostname}:{site_path}"
    resp = requests.get(url, headers=_headers(token), timeout=30)
    resp.raise_for_status()
    site_id = resp.json()["id"]
    logger.debug(f"Site ID: {site_id}")
    return site_id


def _obter_drive_id(token: str, site_id: str) -> str:
    """Obtém o drive ID (document library padrão) do site."""
    url = f"{GRAPH_BASE}/sites/{site_id}/drive"
    resp = requests.get(url, headers=_headers(token), timeout=30)
    resp.raise_for_status()
    drive_id = resp.json()["id"]
    logger.debug(f"Drive ID: {drive_id}")
    return drive_id


class SharePointClient:
    """Cliente para operações no SharePoint via Microsoft Graph API."""

    def __init__(self):
        logger.info(f"Conectando ao SharePoint: {SHAREPOINT_SITE_URL}")
        self._msal_app = _criar_msal_app()
        self.token = _obter_token(self._msal_app)
        self.site_id = _obter_site_id(self.token)
        self.drive_id = _obter_drive_id(self.token, self.site_id)
        logger.info("Conectado ao SharePoint via Microsoft Graph")

    def _renovar_token(self):
        """Renova o access token via MSAL (tokens expiram após ~1h)."""
        logger.debug("Renovando token de acesso...")
        self.token = _obter_token(self._msal_app)

    def _graph_headers(self, extra: dict = None) -> dict:
        h = _headers(self.token)
        if extra:
            h.update(extra)
        return h

    def _garantir_pasta(self):
        """Garante que a pasta destino existe no drive."""
        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{SHAREPOINT_FOLDER_PATH}"
        resp = requests.get(url, headers=self._graph_headers(), timeout=30)
        if resp.status_code == 404:
            logger.info(f"Criando pasta: {SHAREPOINT_FOLDER_PATH}")
            url_criar = f"{GRAPH_BASE}/drives/{self.drive_id}/root/children"
            body = {
                "name": SHAREPOINT_FOLDER_PATH,
                "folder": {},
                "@microsoft.graph.conflictBehavior": "fail",
            }
            resp2 = requests.post(url_criar, headers=self._graph_headers({"Content-Type": "application/json"}), json=body, timeout=30)
            resp2.raise_for_status()
            logger.info(f"Pasta criada: {SHAREPOINT_FOLDER_PATH}")
        elif resp.status_code != 200:
            resp.raise_for_status()

    def listar_arquivos_remotos(self) -> set[str]:
        """Lista todos os arquivos na pasta do SharePoint."""
        self._renovar_token()
        self._garantir_pasta()
        nomes = set()
        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{SHAREPOINT_FOLDER_PATH}:/children?$top=1000&$select=name"

        while url:
            resp = requests.get(url, headers=self._graph_headers(), timeout=30)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("value", []):
                if "file" in item or not item.get("folder"):
                    nomes.add(item["name"])
            url = data.get("@odata.nextLink")

        logger.info(f"Arquivos encontrados no SharePoint: {len(nomes)}")
        return nomes

    def upload_arquivo(self, caminho_local: Path) -> bool:
        """Faz upload de um arquivo. Usa upload session para arquivos grandes.
        Renova token automaticamente se receber 401."""
        nome_arquivo = caminho_local.name
        tamanho = caminho_local.stat().st_size
        tamanho_mb = tamanho / (1024 * 1024)

        for tentativa in range(2):  # max 1 retry após renovar token
            try:
                if tamanho > CHUNK_UPLOAD_THRESHOLD:
                    return self._upload_chunked(caminho_local, nome_arquivo, tamanho, tamanho_mb)
                else:
                    return self._upload_simples(caminho_local, nome_arquivo, tamanho_mb)
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 401 and tentativa == 0:
                    logger.warning(f"Token expirado durante upload de {nome_arquivo}. Renovando...")
                    self._renovar_token()
                    continue
                logger.error(f"FALHA no upload de {nome_arquivo}: {e}")
                return False
            except Exception as e:
                logger.error(f"FALHA no upload de {nome_arquivo}: {e}")
                return False
        return False

    def _upload_simples(self, caminho_local: Path, nome_arquivo: str, tamanho_mb: float) -> bool:
        """Upload simples para arquivos até 4MB."""
        logger.info(f"Upload: {nome_arquivo} ({tamanho_mb:.1f} MB)")
        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{SHAREPOINT_FOLDER_PATH}/{nome_arquivo}:/content"
        with open(caminho_local, "rb") as f:
            resp = requests.put(
                url,
                headers=self._graph_headers({"Content-Type": "application/octet-stream"}),
                data=f,
                timeout=120,
            )
        resp.raise_for_status()
        logger.info(f"OK: {nome_arquivo} enviado")
        return True

    def _upload_chunked(self, caminho_local: Path, nome_arquivo: str, tamanho: int, tamanho_mb: float) -> bool:
        """Upload por sessão para arquivos grandes (>4MB)."""
        logger.info(f"Upload chunked: {nome_arquivo} ({tamanho_mb:.1f} MB)")

        # Criar upload session
        url = f"{GRAPH_BASE}/drives/{self.drive_id}/root:/{SHAREPOINT_FOLDER_PATH}/{nome_arquivo}:/createUploadSession"
        body = {"item": {"@microsoft.graph.conflictBehavior": "replace"}}
        resp = requests.post(
            url,
            headers=self._graph_headers({"Content-Type": "application/json"}),
            json=body,
            timeout=30,
        )
        resp.raise_for_status()
        upload_url = resp.json()["uploadUrl"]

        # Enviar chunks de 10MB
        chunk_size = 10 * 1024 * 1024
        with open(caminho_local, "rb") as f:
            offset = 0
            while offset < tamanho:
                chunk = f.read(chunk_size)
                chunk_len = len(chunk)
                fim = offset + chunk_len - 1

                headers = {
                    "Content-Length": str(chunk_len),
                    "Content-Range": f"bytes {offset}-{fim}/{tamanho}",
                }

                resp = requests.put(upload_url, headers=headers, data=chunk, timeout=300)
                resp.raise_for_status()

                offset += chunk_len
                pct = (offset / tamanho) * 100
                logger.debug(f"  {nome_arquivo}: {pct:.0f}%")

        logger.info(f"OK: {nome_arquivo} enviado")
        return True


def autenticar() -> SharePointClient:
    """Cria e retorna um SharePointClient autenticado."""
    return SharePointClient()


def upload_novos(client: SharePointClient, csvs_para_upload: list[str] | None = None) -> dict:
    """
    Faz upload incremental dos CSVs locais para o SharePoint.

    Args:
        client: SharePointClient autenticado
        csvs_para_upload: Lista de nomes de CSVs para enviar. Se None, envia todos os locais.

    Retorna dict com estatísticas.
    """
    stats = {"verificados": 0, "ja_existentes": 0, "enviados": 0, "erros": 0}

    # Listar o que já existe no SharePoint
    remotos = client.listar_arquivos_remotos()

    # Determinar quais CSVs locais devem ser enviados
    if csvs_para_upload is not None:
        csvs_locais = [DATA_DIR / nome for nome in csvs_para_upload if (DATA_DIR / nome).exists()]
    else:
        csvs_locais = sorted(DATA_DIR.glob("*.csv"))

    logger.info(f"CSVs locais para verificar: {len(csvs_locais)}")

    for csv_path in csvs_locais:
        stats["verificados"] += 1
        nome = csv_path.name

        if nome in remotos:
            stats["ja_existentes"] += 1
            logger.debug(f"SKIP (SharePoint): {nome}")
            continue

        if client.upload_arquivo(csv_path):
            stats["enviados"] += 1
            remotos.add(nome)
        else:
            stats["erros"] += 1

    return stats
