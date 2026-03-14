"""
Transparência Servidores — ETL Pipeline
========================================
Extrai dados de servidores do Portal da Transparência (desde 2020)
e faz upload incremental para pasta no SharePoint.

Uso:
    python -m src.main              # Execução completa (download + upload)
    python -m src.main --download   # Apenas download
    python -m src.main --upload     # Apenas upload
"""

import sys
import argparse
import time
from datetime import datetime

from src.logger import configurar_logger
from src.downloader import descobrir_e_baixar_novos
from src.sharepoint_client import autenticar, upload_novos


def main():
    parser = argparse.ArgumentParser(
        description="ETL: Portal da Transparência → SharePoint"
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Executa apenas o download dos dados do portal",
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Executa apenas o upload para o SharePoint",
    )
    args = parser.parse_args()

    # Se nenhum flag, executa ambos
    executar_download = args.download or (not args.download and not args.upload)
    executar_upload = args.upload or (not args.download and not args.upload)

    logger = configurar_logger()

    logger.info("=" * 60)
    logger.info("TRANSPARÊNCIA SERVIDORES — ETL Pipeline")
    logger.info(f"Início: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    inicio = time.time()
    stats_download = None
    stats_upload = None

    # === FASE 1: Download ===
    if executar_download:
        logger.info("")
        logger.info("-" * 40)
        logger.info("FASE 1: Download do Portal da Transparência")
        logger.info("-" * 40)

        try:
            stats_download = descobrir_e_baixar_novos()
        except Exception as e:
            logger.error(f"Erro na fase de download: {e}", exc_info=True)
            stats_download = {"erros": 1}

    # === FASE 2: Upload para SharePoint ===
    if executar_upload:
        logger.info("")
        logger.info("-" * 40)
        logger.info("FASE 2: Upload para SharePoint")
        logger.info("-" * 40)

        try:
            ctx = autenticar()

            # Se acabamos de baixar, enviar apenas os novos
            novos_csvs = None
            if stats_download and stats_download.get("novos_csvs"):
                novos_csvs = stats_download["novos_csvs"]
                logger.info(f"Enviando {len(novos_csvs)} CSVs novos baixados nesta execução")
            else:
                logger.info("Verificando todos os CSVs locais para upload incremental")

            stats_upload = upload_novos(ctx, novos_csvs)
        except ValueError as e:
            logger.error(f"Configuração SharePoint: {e}")
            stats_upload = {"erros": 1}
        except Exception as e:
            logger.error(f"Erro na fase de upload: {e}", exc_info=True)
            stats_upload = {"erros": 1}

    # === RESUMO ===
    duracao = time.time() - inicio
    logger.info("")
    logger.info("=" * 60)
    logger.info("RESUMO DA EXECUÇÃO")
    logger.info("=" * 60)

    if stats_download:
        logger.info(
            f"Download — Verificados: {stats_download.get('verificados', 0)} | "
            f"Já existentes: {stats_download.get('ja_existentes', 0)} | "
            f"Baixados: {stats_download.get('baixados', 0)} | "
            f"Erros: {stats_download.get('erros', 0)}"
        )

    if stats_upload:
        logger.info(
            f"Upload   — Verificados: {stats_upload.get('verificados', 0)} | "
            f"Já existentes: {stats_upload.get('ja_existentes', 0)} | "
            f"Enviados: {stats_upload.get('enviados', 0)} | "
            f"Erros: {stats_upload.get('erros', 0)}"
        )

    logger.info(f"Duração total: {duracao:.1f}s")
    logger.info("=" * 60)

    # Exit code: 1 se houve erros
    total_erros = (
        (stats_download or {}).get("erros", 0)
        + (stats_upload or {}).get("erros", 0)
    )
    sys.exit(1 if total_erros > 0 else 0)


if __name__ == "__main__":
    main()
