import unicodedata
import re

def normalize_text(text: str) -> str:
    """
    Normaliza texto:
    - Pasa a mayúsculas
    - Elimina acentos
    - Colapsa espacios
    """
    if not text:
        return ""
    text = text.upper()
    text = "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )
    text = re.sub(r"\s+", " ", text)
    return text.strip()

def is_number(value: str) -> bool:
    """
    Verifica si un string representa un número.
    """
    try:
        float(value.replace(",", "").replace(".", "", value.count(".")-1))
        return True
    except Exception:
        return False
