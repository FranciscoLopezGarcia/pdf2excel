import pandas as pd
import logging
import re
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class MacroParser(BaseParser):
    BANK_NAME = "MACRO"
    DETECTION_KEYWORDS = ["BANCO MACRO", "MACRO", "FIDEICOMISO", "CUENTA CORRIENTE ESPECIAL"]

    EXCLUDE_KEYWORDS = [
        "últimos movimientos","cuenta corriente especial","caja de ahorro",
        "tipo","número","moneda","fecha de descarga","operador:","empresa:"
    ]

    DATE_REGEX = r"\d{1,2}/\d{1,2}/\d{4}"
    AMOUNT_REGEX = r"-?\$?\s*\d{1,3}(?:\.\d{3})*,\d{2}|-?\$?\s*\d+,\d{2}"

    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return any(k in haystack for k in self.DETECTION_KEYWORDS)

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        logger.info(f"Procesando {self.BANK_NAME} - {filename}")
        rows = []

        if isinstance(raw_data, str):
            return self._parse_text_lines(raw_data.splitlines())
        elif isinstance(raw_data, list) and len(raw_data) > 0 and hasattr(raw_data[0], "columns"):
            return self._parse_dataframe(raw_data[0])
        elif isinstance(raw_data, list) and all(isinstance(x, str) for x in raw_data):
            return self._parse_text_lines(raw_data)

        return pd.DataFrame(columns=self.REQUIRED_COLUMNS)

    def _parse_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for idx, row in df.iterrows():
            try:
                fecha_str = str(row[0]).strip()
                detalle = str(row[1]).strip()
                debito_str = str(row[-3]).strip() if len(row) >= 3 else ""
                credito_str = str(row[-2]).strip() if len(row) >= 2 else ""
                saldo_str = str(row[-1]).strip() if len(row) >= 1 else ""
                if self._is_valid_row(fecha_str, detalle):
                    rows.append(self._build_row(fecha_str, detalle, debito_str, credito_str, saldo_str))
            except Exception as e:
                logger.warning(f"[{self.BANK_NAME}] Error fila {idx}: {e}")

        return self.finalize(pd.DataFrame(rows))

    def _parse_text_lines(self, lines: list) -> pd.DataFrame:
        rows = []
        regex = re.compile(
            rf"({self.DATE_REGEX})\s+(.+?)\s+({self.AMOUNT_REGEX})?\s+({self.AMOUNT_REGEX})?\s+({self.AMOUNT_REGEX})?"
        )

        for line in lines:
            if self._is_skip_line(line):
                continue
            m = regex.search(line)
            if m:
                fecha, detalle, debito, credito, saldo = m.groups()
                rows.append(self._build_row(fecha, detalle, debito, credito, saldo))

        return self.finalize(pd.DataFrame(rows))

    def _build_row(self, fecha_str, detalle, debito_str, credito_str, saldo_str):
        fecha_norm = normalize_date(fecha_str)
        año, mes = extract_year_month(fecha_norm)
        return {
            "fecha": fecha_norm,
            "mes": mes,
            "año": año,
            "detalle": detalle.strip(),
            "referencia": "",
            "debito": self._parse_amount(debito_str),
            "credito": self._parse_amount(credito_str),
            "saldo": self._parse_amount(saldo_str),
        }

    def _parse_amount(self, val) -> float:
        if not val or str(val).strip() in ["", "-"]:
            return 0.0
        try:
            t = str(val).replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
            neg = t.startswith("-")
            t = t.replace("-", "")
            v = float(t)
            return -v if neg else v
        except Exception:
            return 0.0

    def _is_skip_line(self, line):
        return any(kw in line.lower() for kw in self.EXCLUDE_KEYWORDS)

    def _is_valid_row(self, fecha_str, detalle):
        if not re.match(self.DATE_REGEX, fecha_str):
            return False
        return not any(kw in detalle.lower() for kw in ["fecha", "concepto", "importe", "saldo"])
