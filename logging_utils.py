import logging, sys
from .config import REPORTS_DIR, LOG_FILE


_DEF_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"


_initialized = False


def setup_logging(level=logging.INFO):
    global _initialized
    if _initialized:
        return
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(level)


# Consola
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(level)
    sh.setFormatter(logging.Formatter(_DEF_FORMAT))


# Archivo
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(level)
    fh.setFormatter(logging.Formatter(_DEF_FORMAT))


    root.addHandler(sh)
    root.addHandler(fh)
    _initialized = True