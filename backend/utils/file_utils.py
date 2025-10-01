import os

def get_filename(path: str) -> str:
    """Devuelve solo el nombre del archivo sin extensión."""
    return os.path.splitext(os.path.basename(path))[0]

def detect_bank_from_filename(filename: str, bank_keywords: dict) -> str:
    """
    Detecta banco según filename.
    bank_keywords: dict {"SANTANDER": ["SANTANDER", "RIO"], ...}
    """
    fname = filename.upper()
    for bank, keywords in bank_keywords.items():
        for kw in keywords:
            if kw in fname:
                return bank
    return "GENERIC"
