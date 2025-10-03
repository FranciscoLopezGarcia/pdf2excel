import pandas as pd
import re
import logging
from datetime import datetime
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class PatagoniaParser(BaseParser):
    BANK_NAME = "PATAGONIA"
    DETECTION_KEYWORDS = ["BANCO PATAGONIA", "PATAGONIA", "ESTADO DE CUENTAS UNIFICADO"]

    DATE_REGEX = r"\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?"
    AMOUNT_REGEX = r"\d{1,3}(?:\.\d{3})*,\d{2}-?|\d+,\d{2}-?"

    EXCLUDE_KEYWORDS = [
        "estimado cliente","situacion impositiva","responsable inscripto","ingresos brutos",
        "cbu:","pagina:","p£gina:"
    ]

    def detect(self, text: str, filename: str = "") -> bool:
        t = (text or "").upper()
        f = (filename or "").upper()
        return any(k in t or k in f for k in self.DETECTION_KEYWORDS)

    def parse(self, raw_data, filename=""):
        logger.info(f"Procesando {self.BANK_NAME} - {filename}")
        rows = []

        if isinstance(raw_data, str):
            return self._parse_text_lines(raw_data.splitlines())
        elif isinstance(raw_data, list) and len(raw_data) > 0 and all(isinstance(x, str) for x in raw_data):
            return self._parse_text_lines(raw_data)
        return pd.DataFrame(columns=self.REQUIRED_COLUMNS)

    def _parse_text_lines(self, lines):
        rows = []
        patt = re.compile(
            rf"(?P<fecha>{self.DATE_REGEX})\s+(?P<detalle>.+?)\s+(?P<deb>{self.AMOUNT_REGEX})?\s+(?P<cred>{self.AMOUNT_REGEX})?\s+(?P<saldo>{self.AMOUNT_REGEX})$",
            re.I,
        )

        saldo_anterior, saldo_final = None, None

        for line in lines:
            clean = re.sub(r"\s+", " ", line).strip()
            if not clean or self._is_skip_line(clean):
                continue

            if "SALDO ANTERIOR" in clean.upper():
                m = re.search(rf"({self.AMOUNT_REGEX})$", clean)
                if m:
                    saldo_anterior = self._parse_amount(m.group(1))
                continue
            if "SALDO ACTUAL" in clean.upper() or "SALDO FINAL" in clean.upper():
                m = re.search(rf"({self.AMOUNT_REGEX})$", clean)
                if m:
                    saldo_final = self._parse_amount(m.group(1))
                continue

            m = patt.match(clean)
            if m:
                g = m.groupdict()
                fecha = normalize_date(g.get("fecha", ""))
                año, mes = extract_year_month(fecha)
                detalle = g.get("detalle", "")
                deb = self._parse_amount(g.get("deb"))
                cred = self._parse_amount(g.get("cred"))
                saldo = self._parse_amount(g.get("saldo"))
                rows.append({
                    "fecha": fecha,
                    "mes": mes,
                    "año": año,
                    "detalle": detalle,
                    "referencia": "",
                    "debito": deb,
                    "credito": cred,
                    "saldo": saldo,
                })

        # inyectar saldo anterior/final
        if saldo_anterior is not None:
            rows.insert(0, {
                "fecha": "", "mes": "", "año": "",
                "detalle": "SALDO ANTERIOR", "referencia": "",
                "debito": 0.0, "credito": 0.0, "saldo": saldo_anterior
            })
        if saldo_final is not None:
            rows.append({
                "fecha": "", "mes": "", "año": "",
                "detalle": "SALDO FINAL", "referencia": "",
                "debito": 0.0, "credito": 0.0, "saldo": saldo_final
            })

        return self.finalize(pd.DataFrame(rows))

    def _parse_amount(self, s: str) -> float:
        if not s or s.strip() in ["", "-"]:
            return 0.0
        t = str(s).replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
        neg = t.startswith("-") or t.endswith("-") or ("(" in t and ")" in t)
        t = t.replace("-", "").replace("(", "").replace(")", "")
        try:
            v = float(t)
            return -v if neg else v
        except Exception:
            return 0.0

    def _is_skip_line(self, line):
        return any(kw in line.lower() for kw in self.EXCLUDE_KEYWORDS)
