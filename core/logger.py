import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path


LOG_DIR = Path(__file__).resolve().parent.parent / "log"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "af_crawl.log"


class _ColoredFormatter(logging.Formatter):
    COLOR_MAP = {
        logging.DEBUG: "\033[36m",    # Cyan
        logging.INFO: "\033[32m",     # Green
        logging.WARNING: "\033[33m",  # Yellow
        logging.ERROR: "\033[31m",    # Red
        logging.CRITICAL: "\033[41m", # Red background
    }

    RESET = "\033[0m"

    def format(self, record):
        color = self.COLOR_MAP.get(record.levelno, "")
        message = super().format(record)
        if color:
            message = f"{color}{message}{self.RESET}"
        return message


def setup_logging(level: int = logging.INFO):
    fmt = "%(asctime)s [%(levelname)s] %(name)s - %(message)s"

    # 控制台
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(_ColoredFormatter(fmt))

    # 文件
    file_handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(fmt))

    logging.basicConfig(level=level, handlers=[console_handler, file_handler])


# 默认初始化
setup_logging() 