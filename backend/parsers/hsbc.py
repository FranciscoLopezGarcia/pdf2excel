import logging
import re
from datetime import datetime
import pandas as pd
from parsers.base_parser import BaseParser

logger = logging.getLogger(__name__)

BANK_NAME = "HSBC"
DETECTION_KEYWORDS = ["HSBC", "HSBC ARGENTINA", "HSBC BANK"]

_AMT = r"[\-\(\)]?\d{1,3}(?:[\.\s]\d{3})*(?:,\d{2})?[\)]?"
_AMT_STRICT = r"-?\d{1,3}(?:\.\d{3})*,\d{2}"
_DATE = r"(?:\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?)"


def _to_amount(raw: str) -> float:
    if not raw:
        return 0.0
    s = raw.strip()
    if not s or s in ["-", "nan"]:
        return 0.0
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    if s.startswith("-"):
        neg = True
        s = s[1:]
    s = s.replace(".", "").replace(" ", "").replace("\u00a0", "")
    s = s.replace(",", ".")
    try:
        val = float(s)
    except Exception:
        return 0.0
    return -val if neg else val


def _norm_date(raw: str, year_hint: int = None) -> str:
    if not raw:
        return ""
    raw = raw.strip().replace(".", "/").replace("-", "/")
    parts = raw.split("/")
    if len(parts) == 2:
        d, m = parts
        y = year_hint or datetime.now().year
        try:
            return datetime(int(y), int(m), int(d)).strftime("%d/%m/%Y")
        except Exception:
            return raw
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
        except Exception:
            continue
    return raw


class HSBCParser(BaseParser):
    BANK_NAME = BANK_NAME
    DETECTION_KEYWORDS = DETECTION_KEYWORDS
    PREFER_TABLES = True
    OUTPUT_COLUMNS = ["fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"]

    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return any(k in haystack for k in self.DETECTION_KEYWORDS)

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        logger.info(f"Procesando extracto {self.BANK_NAME} - {filename}")

        if isinstance(raw_data, str):
            lines = raw_data.splitlines()
        elif isinstance(raw_data, list) and all(isinstance(x, str) for x in raw_data):
            lines = raw_data
        else:
            if isinstance(raw_data, list) and len(raw_data) > 0 and hasattr(raw_data[0], "columns"):
                return self._parse_dataframe(raw_data[0])
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        return self._parse_text_lines(lines)

    def _parse_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame()
        out["fecha"] = df.iloc[:, 0].astype(str).apply(_norm_date)
        out["detalle"] = df.iloc[:, 1].astype(str)
        out["debito"] = df.iloc[:, -3].apply(_to_amount)
        out["credito"] = df.iloc[:, -2].apply(_to_amount)
        out["saldo"] = df.iloc[:, -1].apply(_to_amount)
        out["referencia"] = ""
        return self.finalize(out)

    def _parse_text_lines(self, lines: list) -> pd.DataFrame:
        patt = re.compile(
            rf"^(?P<fecha>{_DATE})\s+(?P<detalle>.+?)\s+(?P<deb>{_AMT})?\s+(?P<cred>{_AMT})?\s+(?P<saldo>{_AMT})$",
            re.I,
        )

        rows = []
        buffer = []
        current = None
        year_hint = self._infer_year("\n".join(lines))

        saldo_anterior = None
        saldo_final = None

        for ln in lines:
            if re.search(r"SALDO\s+ANTERIOR", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_anterior = _to_amount(m.group(1))
            if re.search(r"SALDO\s+FINAL|SALDO\s+AL", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_final = _to_amount(m.group(1))

        for ln in lines:
            if re.search(r"FECHA\s+.*SALDO", ln, re.I):
                continue
            m = patt.match(ln)
            if m:
                if current:
                    current["detalle"] += " " + " ".join(buffer).strip()
                    rows.append(current)
                    buffer = []
                g = m.groupdict()
                fecha = _norm_date(g.get("fecha", ""), year_hint)
                detalle = g.get("detalle", "")
                deb = _to_amount(g.get("deb") or "0")
                cred = _to_amount(g.get("cred") or "0")
                saldo = _to_amount(g.get("saldo") or "0")
                current = {"fecha": fecha, "detalle": detalle, "referencia": "", "debito": deb, "credito": cred, "saldo": saldo}
            else:
                if current:
                    buffer.append(ln)

        if current:
            current["detalle"] += " " + " ".join(buffer).strip()
            rows.append(current)

        if saldo_anterior is not None:
            rows.insert(0, {"fecha": "", "detalle": "SALDO ANTERIOR", "referencia": "", "debito": 0, "credito": 0, "saldo": saldo_anterior})
        if saldo_final is not None:
            rows.append({"fecha": "", "detalle": "SALDO FINAL", "referencia": "", "debito": 0, "credito": 0, "saldo": saldo_final})

        return self.finalize(pd.DataFrame(rows))
