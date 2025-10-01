import logging
import sys

def setup_logger(level=logging.INFO):
    """
    Configura logging con formato estándar para todo el proyecto.
    """
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = []  # evita duplicados
    root.addHandler(handler)

    return root
