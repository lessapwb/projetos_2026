import io
import re
import time
import zipfile
import logging
import requests
from pathlib import Path
from datetime import datetime
from tqdm import tqdm

from src.config import (
    TRANSPARENCIA_PAGE_URL,
    CDN_BASE_URL,
    DATA_DIR,
    HEADERS,
    ANO_INICIO,
    DOWNLOAD_TIMEOUT,
    DOWNLOAD_CHUNK_SIZE,
    MAX_RETRIES,
    carregar_downloads_concluidos,
    salvar_downloads_concluidos,
)

logger = logging.getLogger("transparencia_etl")


def obter_arquivos_disponiveis(desde_ano: int = ANO_INICIO) -> list[dict]:
    """
    Faz scraping da página do portal para obter a lista real de arquivos disponíveis.
    Retorna lista de dicts: {"ano": "2020", "mes": "01", "origem": "Servidores_SIAPE"}
    """
    logger.info("Consultando portal para obter arquivos disponíveis...")
    resp = requests.get(TRANSPARENCIA_PAGE_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()

    # Extrair o array JavaScript "arquivos" embutido na página
    entries = re.findall(
        r'\{"ano"\s*:\s*"(\d+)"\s*,\s*"mes"\s*:\s*"(\d+)"\s*,\s*"dia"\s*:\s*"[^"]*"\s*,\s*"origem"\s*:\s*"([^"]*)"\}',
        resp.text,
    )

    if not entries:
        logger.error("Não foi possível extrair a lista de arquivos do portal!")
        return []

    # Filtrar desde o ano desejado
    arquivos = [
        {"ano": ano, "mes": mes, "origem": origem}
        for ano, mes, origem in entries
        if int(ano) >= desde_ano
    ]

    tipos_unicos = sorted(set(a["origem"] for a in arquivos))
    logger.info(f"Encontrados {len(arquivos)} arquivos disponíveis desde {desde_ano}")
    logger.info(f"Tipos de planilha: {', '.join(tipos_unicos)}")

    return arquivos


def _construir_url_cdn(ano: str, mes: str, origem: str) -> str:
    """Constrói a URL direta do CDN para download."""
    return f"{CDN_BASE_URL}/{ano}{mes}_{origem}.zip"


def _gerar_chave(arquivo: dict) -> str:
    """Gera chave única para rastreamento: YYYYMM_Tipo."""
    return f"{arquivo['ano']}{arquivo['mes']}_{arquivo['origem']}"


def baixar_e_extrair(arquivo: dict) -> list[Path]:
    """
    Baixa o ZIP do CDN e extrai os CSVs para DATA_DIR.
    Retorna lista de paths dos CSVs extraídos.
    """
    chave = _gerar_chave(arquivo)
    url = _construir_url_cdn(arquivo["ano"], arquivo["mes"], arquivo["origem"])

    for tentativa in range(1, MAX_RETRIES + 1):
        try:
            logger.info(f"Baixando {chave} (tentativa {tentativa}/{MAX_RETRIES})...")

            resp = requests.get(
                url,
                headers=HEADERS,
                timeout=DOWNLOAD_TIMEOUT,
                stream=True,
            )
            resp.raise_for_status()

            # Baixar com barra de progresso
            tamanho_total = int(resp.headers.get("content-length", 0))
            conteudo = bytearray()

            with tqdm(
                total=tamanho_total,
                unit="B",
                unit_scale=True,
                desc=chave,
                disable=tamanho_total == 0,
            ) as barra:
                for chunk in resp.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                    conteudo.extend(chunk)
                    barra.update(len(chunk))

            conteudo_bytes = bytes(conteudo)

            # Extrair CSVs do ZIP
            arquivos_extraidos = []
            with zipfile.ZipFile(io.BytesIO(conteudo_bytes)) as zf:
                csvs = [n for n in zf.namelist() if n.lower().endswith(".csv")]
                if not csvs:
                    logger.warning(f"Nenhum CSV encontrado no ZIP de {chave}")
                    return []

                for csv_nome in csvs:
                    nome_destino = Path(csv_nome).name
                    destino = DATA_DIR / nome_destino

                    with zf.open(csv_nome) as origem:
                        destino.write_bytes(origem.read())

                    arquivos_extraidos.append(destino)
                    logger.debug(f"  Extraído: {destino.name}")

            logger.info(f"OK: {chave} — {len(arquivos_extraidos)} CSV(s) extraído(s)")
            return arquivos_extraidos

        except zipfile.BadZipFile:
            logger.error(f"ZIP corrompido para {chave}")
            return []
        except requests.RequestException as e:
            logger.warning(f"Erro no download de {chave}: {e}")
            if tentativa < MAX_RETRIES:
                espera = tentativa * 10
                logger.info(f"Aguardando {espera}s antes de nova tentativa...")
                time.sleep(espera)
            else:
                logger.error(f"FALHA: {chave} após {MAX_RETRIES} tentativas")
                return []

    return []


def descobrir_e_baixar_novos() -> dict:
    """
    Fluxo principal de download:
    1. Scraping da página para obter arquivos disponíveis desde ANO_INICIO
    2. Compara com registro de downloads já concluídos
    3. Baixa apenas os novos

    Retorna dict com estatísticas.
    """
    stats = {
        "verificados": 0,
        "ja_existentes": 0,
        "baixados": 0,
        "erros": 0,
        "novos_csvs": [],
    }

    # Obter lista real de arquivos disponíveis no portal
    disponiveis = obter_arquivos_disponiveis()
    if not disponiveis:
        logger.error("Nenhum arquivo disponível encontrado no portal.")
        return stats

    # Carregar registro de downloads já feitos
    concluidos = carregar_downloads_concluidos()
    stats["verificados"] = len(disponiveis)
    stats["ja_existentes"] = sum(1 for a in disponiveis if _gerar_chave(a) in concluidos)

    pendentes = [a for a in disponiveis if _gerar_chave(a) not in concluidos]
    logger.info(
        f"Total disponível: {len(disponiveis)} | "
        f"Já baixados: {stats['ja_existentes']} | "
        f"Pendentes: {len(pendentes)}"
    )

    for i, arquivo in enumerate(pendentes, 1):
        chave = _gerar_chave(arquivo)
        logger.info(f"[{i}/{len(pendentes)}] Processando {chave}...")

        csvs_extraidos = baixar_e_extrair(arquivo)
        if csvs_extraidos:
            stats["baixados"] += len(csvs_extraidos)
            for csv_path in csvs_extraidos:
                stats["novos_csvs"].append(csv_path.name)

            # Registrar como concluído
            concluidos[chave] = {
                "data_download": datetime.now().isoformat(),
                "csvs": [p.name for p in csvs_extraidos],
            }
            salvar_downloads_concluidos(concluidos)
        else:
            stats["erros"] += 1

    return stats
