import logging
from datetime import datetime
from src.config import LOGS_DIR


def configurar_logger(nome: str = "transparencia_etl") -> logging.Logger:
    """Configura logger com output no console e em arquivo."""
    logger = logging.getLogger(nome)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # Formato
    formato = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler: Console
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formato)
    logger.addHandler(console_handler)

    # Handler: Arquivo
    data_atual = datetime.now().strftime("%Y%m%d")
    arquivo_log = LOGS_DIR / f"execucao_{data_atual}.log"
    file_handler = logging.FileHandler(arquivo_log, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formato)
    logger.addHandler(file_handler)

    return logger
