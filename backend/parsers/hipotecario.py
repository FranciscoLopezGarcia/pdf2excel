import pandas as pd
import logging
import re
from datetime import datetime
from .base_parser import BaseParser

logger = logging.getLogger(__name__)

class HipotecarioParser(BaseParser):
    BANK_NAME = "HIPOTECARIO"
    PREFER_TABLES = True
    DETECTION_KEYWORDS = ["BANCO HIPOTECARIO", "HIPOTECARIO"]

    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return any(kw in haystack for kw in self.DETECTION_KEYWORDS)

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
        out["fecha"] = df.iloc[:, 0].astype(str).apply(self._norm_date)
        out["detalle"] = df.iloc[:, 1].astype(str)
        out["debito"] = df.iloc[:, -3].apply(self._parse_amount)
        out["credito"] = df.iloc[:, -2].apply(self._parse_amount)
        out["saldo"] = df.iloc[:, -1].apply(self._parse_amount)
        out["referencia"] = ""
        return self.finalize(out)

    def _parse_text_lines(self, lines: list) -> pd.DataFrame:
        rows = []
        regex = re.compile(
            r"(?P<fecha>\d{2}[/-]\d{2}[/-]\d{2,4})\s+"
            r"(?P<detalle>.+?)\s+"
            r"(?P<debito>[\d\.,\-]+)?\s+"
            r"(?P<credito>[\d\.,\-]+)?\s+"
            r"(?P<saldo>[\d\.,\-]+)?$"
        )

        buffer_concept = []
        current_row = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            match = regex.match(line)
            if match:
                # Si había una transacción en construcción, la guardamos
                if current_row:
                    current_row["detalle"] = " ".join(buffer_concept).strip()
                    rows.append(current_row)
                    buffer_concept = []

                fecha = self._norm_date(match.group("fecha"))
                detalle = match.group("detalle")
                debito = self._parse_amount(match.group("debito"))
                credito = self._parse_amount(match.group("credito"))
                saldo = self._parse_amount(match.group("saldo"))

                current_row = {
                    "fecha": fecha,
                    "detalle": detalle.strip(),
                    "debito": debito,
                    "credito": credito,
                    "saldo": saldo,
                    "referencia": ""
                }
            else:
                # Línea adicional del concepto (multilínea)
                if current_row:
                    buffer_concept.append(line)

        # Agregar la última transacción
        if current_row:
            if buffer_concept:
                current_row["detalle"] += " " + " ".join(buffer_concept).strip()
            rows.append(current_row)

        return self.finalize(pd.DataFrame(rows))

    def _parse_amount(self, s: str) -> float:
        if not s or s.strip() in ["", "-"]:
            return 0.0
        t = s.replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
        neg = t.startswith("-") or t.endswith("-")
        t = t.replace("-", "")
        try:
            v = float(t)
            return -v if neg else v
        except Exception:
            logger.warning(f"[{self.BANK_NAME}] No se pudo parsear monto: {s}")
            return 0.0

    def _norm_date(self, raw: str) -> str:
        if not raw:
            return ""
        raw = raw.strip().replace("-", "/")
        for fmt in ("%d/%m/%Y", "%d/%m/%y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%d/%m/%Y")
            except Exception:
                continue
        return raw
