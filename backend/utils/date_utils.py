from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def normalize_date(date_str: str, inferred_year=None) -> str:
    """
    Normaliza fechas a formato YYYY-MM-DD.
    - Soporta dd/mm/yy, dd-mm-yyyy, yyyy/mm/dd
    - Si falta el año, usa inferred_year o el actual
    """
    if not date_str:
        return ""

    date_str = str(date_str).strip()
    formats = ["%d/%m/%Y", "%d/%m/%y", "%Y/%m/%d", "%d-%m-%Y", "%d-%m-%y"]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            continue

    # Caso dd/mm → completar año
    if "/" in date_str and len(date_str.split("/")) == 2:
        day, month = date_str.split("/")
        year = inferred_year or datetime.now().year
        try:
            dt = datetime(year=int(year), month=int(month), day=int(day))
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass

    logger.warning(f"No se pudo normalizar fecha: {date_str}")
    return date_str

def extract_year_month(date_str: str):
    """
    Devuelve (año, mes) a partir de una fecha string.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.year, dt.month
    except Exception:
        return None, None
