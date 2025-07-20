import logging
import os

def init(log_file, level = logging.ERROR, cmd_show = False):
    log_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), log_file)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    handlers = [
        logging.FileHandler(log_file, mode='a', encoding='utf-8'),
    ]
    if cmd_show:
        handlers.append(logging.StreamHandler())
    # 设置日志
    logging.basicConfig(
        level=level, 
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers
    )

_header = "AF_Crawler"

def set_header(header):
    global _header
    _header = header

def info(msg):
    logging.info(f"[{_header}] {msg}")

def warning(msg):
    logging.warning(f"[{_header}] {msg}")

def error(msg):
    logging.error(f"[{_header}] {msg}")
    
def debug(msg):
    logging.debug(f"[{_header}] {msg}")