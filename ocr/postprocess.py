# Limpiezas mínimas de texto OCR; ampliable


def sanitize_text(s: str) -> str:
    if not isinstance(s, str):
        try:
            s = s.decode('utf-8', errors='ignore')
        except Exception:
            s = str(s)
# normalizaciones simples
    return s.replace('\r', ' ').replace('\t', ' ')