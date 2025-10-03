import pandas as pd
import re
import logging
from datetime import datetime
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

BANK_NAME = "PROVINCIA"
DETECTION_KEYWORDS = ["BANCO PROVINCIA", "PROVINCIA"]

_AMT = r"[\-\(\)]?\d{1,3}(?:[\.\s]\d{3})*(?:,\d{2})?[\)]?"
_AMT_STRICT = r"-?\d{1,3}(?:\.\d{3})*,\d{2}"
_DATE = r"(?:\d{1,2}[\/-]\d{1,2}(?:[\/-]\d{2,4})?)"


def _to_float(value: str) -> float:
    """Convierte string a float (formato argentino)."""
    if not value or value.strip() in ["", "-", "nan"]:
        return 0.0
    try:
        clean = value.strip().replace("$", "").replace(" ", "").replace("\u00a0", "")
        neg = clean.startswith("-")
        clean = clean.replace("-", "").replace(".", "").replace(",", ".")
        v = float(clean)
        return -v if neg else v
    except Exception:
        return 0.0


def _norm_date(raw: str, year_hint: int = None) -> str:
    if not raw:
        return ""
    raw = raw.strip().replace("-", "/").replace(".", "/")
    parts = raw.split("/")
    if len(parts) == 2:  # completar año faltante
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


class ProvinciaParser(BaseParser):
    """
    Parser específico para Banco Provincia.
    Característica: columna "Importe" única (negativos = débitos, positivos = créditos).
    """

    BANK_NAME = BANK_NAME
    DETECTION_KEYWORDS = DETECTION_KEYWORDS
    OUTPUT_COLUMNS = ["fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"]

    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return any(k in haystack for k in self.DETECTION_KEYWORDS)

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        logger.info(f"Procesando extracto Banco Provincia: {filename}")

        if isinstance(raw_data, str):
            lines = raw_data.splitlines()
        elif isinstance(raw_data, list) and all(isinstance(x, str) for x in raw_data):
            lines = raw_data
        elif isinstance(raw_data, list) and len(raw_data) > 0 and hasattr(raw_data[0], "columns"):
            return self._parse_dataframe(raw_data[0])
        else:
            return pd.DataFrame(columns=self.OUTPUT_COLUMNS)

        return self._parse_text_lines(lines)

    def _parse_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in df.iterrows():
            try:
                fecha_str = str(row[0]).strip()
                concepto = str(row[1]).strip()
                importe_str = str(row[2]).strip()
                saldo_str = str(row[4]).strip() if len(row) > 4 else ""
                parsed_row = self._build_row(fecha_str, concepto, importe_str, saldo_str)
                if parsed_row:
                    rows.append(parsed_row)
            except Exception as e:
                logger.warning(f"[{self.BANK_NAME}] Error procesando fila: {e}")
        return self.finalize(pd.DataFrame(rows))

    def _parse_text_lines(self, lines: list) -> pd.DataFrame:
        patt = re.compile(
            rf"^(?P<fecha>{_DATE})\s+(?P<detalle>.+?)\s+(?P<importe>{_AMT})\s+(?:{_DATE})?\s+(?P<saldo>{_AMT})$",
            re.I,
        )

        rows = []
        year_hint = self._infer_year("\n".join(lines))

        saldo_anterior = None
        saldo_final = None

        for ln in lines:
            if re.search(r"SALDO\s+ANTERIOR", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_anterior = _to_float(m.group(1))
            if re.search(r"SALDO\s+FINAL|SALDO\s+AL", ln, re.I):
                m = re.search(rf"({_AMT_STRICT})$", ln)
                if m:
                    saldo_final = _to_float(m.group(1))

        for ln in lines:
            line = ln.strip()
            if not line or self._is_skip_line(line):
                continue
            m = patt.match(line)
            if not m:
                continue
            g = m.groupdict()
            fecha = _norm_date(g.get("fecha", ""), year_hint)
            detalle = self._clean_concept(g.get("detalle", ""))
            importe = _to_float(g.get("importe") or "0")
            saldo = _to_float(g.get("saldo") or "0")
            debito, credito = (abs(importe), 0.0) if importe < 0 else (0.0, importe)
            rows.append(
                {
                    "fecha": fecha,
                    "mes": fecha.split("/")[1] if fecha else "",
                    "año": fecha.split("/")[2] if fecha else "",
                    "detalle": detalle,
                    "referencia": "",
                    "debito": debito,
                    "credito": credito,
                    "saldo": saldo,
                }
            )

        if saldo_anterior is not None:
            rows.insert(0, {"fecha": "", "mes": "", "año": "", "detalle": "SALDO ANTERIOR", "referencia": "", "debito": 0, "credito": 0, "saldo": saldo_anterior})
        if saldo_final is not None:
            rows.append({"fecha": "", "mes": "", "año": "", "detalle": "SALDO FINAL", "referencia": "", "debito": 0, "credito": 0, "saldo": saldo_final})

        return self.finalize(pd.DataFrame(rows))

    def _clean_concept(self, concepto: str) -> str:
        concepto = re.sub(r"\s+", " ", concepto).strip()
        concepto = re.sub(r"PERIODO\s+DESDE.+?HASTA.+?\d{4}", "", concepto, flags=re.I)
        return concepto[:200]

    def _is_skip_line(self, line: str) -> bool:
        l = line.lower()
        skip = ["extracto de cuenta", "banco provincia", "cuenta corriente", "emitido el", "frecuencia", "hoja", "cbu:", "total retención"]
        return any(kw in l for kw in skip)
