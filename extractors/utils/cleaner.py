# extractors/utils/cleaner.py
import re
from datetime import datetime

def clean_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    # replace weird characters
    s = s.replace("\xa0", " ").replace("\u200b", "")
    s = re.sub(r"\s+", " ", s)
    s = s.strip()
    return s

def normalize_date(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    # try dd/mm/yyyy or dd/mm/yy
    for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%d.%m.%Y", "%d.%m.%y"):
        try:
            d = datetime.strptime(s, fmt)
            return d.strftime("%d/%m/%Y")
        except Exception:
            continue
    # fallback: try find pattern inside
    m = re.search(r"(\d{1,2}[\/\-\.\s]\d{1,2}[\/\-\.\s]\d{2,4})", s)
    if m:
        try:
            for fmt in ("%d/%m/%Y", "%d/%m/%y", "%d-%m-%Y", "%d-%m-%y", "%d.%m.%Y", "%d.%m.%y"):
                try:
                    d = datetime.strptime(m.group(1).replace("-", "/").replace(".", "/").strip(), fmt)
                    return d.strftime("%d/%m/%Y")
                except:
                    continue
        except:
            pass
    return s

def parse_amount(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip()
    # remove spaces
    s = s.replace(" ", "")
    # remove currency symbols
    s = re.sub(r"[^\d\-\.,]", "", s)
    if s.count(",") > 0 and s.count(".") > 0:
        # Heuristic: if comma appears after dot or thousands vs decimals
        # If last separator is comma, treat comma as decimal separator
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "")
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    else:
        # only commas: treat comma as decimal separator if there are two decimals
        if s.count(",") == 1 and len(s.split(",")[-1]) == 2:
            s = s.replace(",", ".")
        else:
            s = s.replace(",", "")
    if s in ("", "-", "+"):
        return 0.0
    try:
        return float(s)
    except:
        # fallback: strip non digits
        cleaned = re.sub(r"[^\d\-.]", "", s)
        try:
            return float(cleaned)
        except:
            return 0.0

def format_amount(num: float) -> str:
    try:
        neg = num < 0
        num = abs(num)
        # format with thousands and comma as decimal
        s = f"{num:,.2f}"
        s = s.replace(",", "X").replace(".", ",").replace("X", ".")
        return ("-" + s) if neg else s
    except:
        return str(num)
