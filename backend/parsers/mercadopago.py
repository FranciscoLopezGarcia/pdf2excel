import pandas as pd
import logging
import re
from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class MercadoPagoParser(BaseParser):
    BANK_NAME = "MERCADOPAGO"
    PREFER_TABLES = True
    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return "MERCADO PAGO" in haystack or "MERCADOPAGO" in haystack

    DETECTION_KEYWORDS = ["MERCADO PAGO", "MERCADOPAGO"]

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        logger.info(f"Procesando {self.BANK_NAME} - {filename}")
        if isinstance(raw_data, str):
            return self._parse_text_lines(raw_data.splitlines())
        elif isinstance(raw_data, list) and len(raw_data) > 0 and hasattr(raw_data[0], "columns"):
            return self._parse_dataframe(raw_data[0])
        elif isinstance(raw_data, list) and all(isinstance(x, str) for x in raw_data):
            return self._parse_text_lines(raw_data)
        return pd.DataFrame(columns=self.REQUIRED_COLUMNS)

    def _parse_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        out = pd.DataFrame()
        out["fecha"] = df.iloc[:, 0].astype(str)
        out["detalle"] = df.iloc[:, 1].astype(str)
        out["debito"] = pd.to_numeric(df.iloc[:, -3], errors="coerce").fillna(0.0)
        out["credito"] = pd.to_numeric(df.iloc[:, -2], errors="coerce").fillna(0.0)
        out["saldo"] = pd.to_numeric(df.iloc[:, -1], errors="coerce").fillna(0.0)
        out["referencia"] = ""
        return self.finalize(out)

    def _parse_text_lines(self, lines: list) -> pd.DataFrame:
        rows, regex = [], re.compile(
            r"(\d{2}[/-]\d{2}[/-]\d{2,4})\s+(.+?)\s+([\d\.,]+)?\s+([\d\.,]+)?\s+([\d\.,]+)?"
        )
        for line in lines:
            match = regex.search(line)
            if match:
                fecha, detalle, debito, credito, saldo = match.groups()
                rows.append({
                    "fecha": self.normalize_date(fecha),
                    "detalle": detalle.strip(),
                    "debito": self._to_amount(debito),
                    "credito": self._to_amount(credito),
                    "saldo": self._to_amount(saldo),
                    "referencia": ""
                })
        return self.finalize(pd.DataFrame(rows))

    def _to_amount(self, val) -> float:
        if not val:
            return 0.0
        try:
            return float(str(val).replace(".", "").replace(",", "."))
        except Exception:
            return 0.0
