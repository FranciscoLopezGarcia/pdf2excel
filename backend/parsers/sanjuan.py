import pandas as pd
import re
import logging
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class SanJuanParser(BaseParser):
    BANK_NAME = "SANJUAN"
    DETECTION_KEYWORDS = ["BANCO SAN JUAN", "SAN JUAN", "HOME BANKING", "RESUMEN MENSUAL"]

    EXCLUDE_KEYWORDS = [
        "home banking","probalo","terminales de autoservicio","contact center",
        "detalle de las cuentas","detalle de titulares de cuentas",
        "movimientos pendientes","detalle de impuestos","cbu","cuit","producto","periodo del"
    ]

    DATE_REGEX = r"\d{1,2}/\d{1,2}/\d{2,4}"
    AMOUNT_REGEX = r"\d{1,3}(?:\.\d{3})*,\d{2}|\d+,\d{2}|(?<!\d),\d{2}"

    def parse(self, raw_data, filename=""):
        logger.info(f"Procesando extracto Banco San Juan: {filename}")
        rows = []
        if isinstance(raw_data, list) and len(raw_data) > 0:
            if isinstance(raw_data[0], pd.DataFrame):
                for df in raw_data:
                    rows.extend(self._parse_dataframe(df))
            else:
                rows.extend(self._parse_text_lines(raw_data))
        df = pd.DataFrame(rows, columns=[
            "fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"
        ])
        return self.finalize(df)

    def _parse_dataframe(self, df):
        rows = []
        for idx, row in df.iterrows():
            try:
                if len(row) >= 4:
                    fecha_str = str(row[0]).strip()
                    concepto = str(row[1]).strip()
                    debito_str = str(row[2]).strip()
                    credito_str = str(row[3]).strip()
                    saldo_str = str(row[4]).strip() if len(row) > 4 else ""
                    if self._is_valid_row(fecha_str, concepto):
                        parsed = self._build_row(fecha_str, concepto, debito_str, credito_str, saldo_str)
                        if parsed:
                            rows.append(parsed)
            except Exception as e:
                logger.warning(f"[{self.BANK_NAME}] Error fila {idx}: {e}")
        return rows

    def _parse_text_lines(self, lines):
        rows = []
        pattern = re.compile(
            rf"({self.DATE_REGEX})\s+(.+?)\s+({self.AMOUNT_REGEX})?\s+({self.AMOUNT_REGEX})?\s+({self.AMOUNT_REGEX})$"
        )
        for line in lines:
            line = line.strip()
            if not line or self._is_skip_line(line):
                continue
            m = pattern.search(line)
            if m:
                fecha_str, concepto, debito_str, credito_str, saldo_str = m.groups()
                parsed = self._build_row(fecha_str, concepto, debito_str, credito_str, saldo_str)
                if parsed:
                    rows.append(parsed)
        return rows

    def _build_row(self, fecha_str, concepto, debito_str, credito_str, saldo_str):
        try:
            fecha_norm = normalize_date(fecha_str)
            if not fecha_norm:
                return None
            año, mes = extract_year_month(fecha_norm)
            referencia = self._extract_reference(concepto)
            concepto_limpio = self._clean_concept(concepto)
            return {
                "fecha": fecha_norm,
                "mes": mes,
                "año": año,
                "detalle": concepto_limpio,
                "referencia": referencia,
                "debito": self._parse_amount(debito_str),
                "credito": self._parse_amount(credito_str),
                "saldo": self._parse_amount(saldo_str),
            }
        except Exception as e:
            logger.warning(f"[{self.BANK_NAME}] Error construyendo fila: {e}")
            return None

    def _extract_reference(self, concepto):
        m = re.search(r"NRO\.?(\d+)", concepto, re.I)
        return m.group(1) if m else ""

    def _clean_concept(self, concepto):
        concepto = re.sub(r"\d{11}", "", concepto)
        concepto = re.sub(r"NRO\.?\d+", "", concepto, flags=re.I)
        return re.sub(r"\s+", " ", concepto).strip()

    def _is_valid_row(self, fecha_str, concepto):
        if not re.match(self.DATE_REGEX, fecha_str):
            return False
        concepto_lower = concepto.lower()
        skip = ["fecha", "concepto", "debito", "credito", "saldo", "subtotal", "transporte"]
        return not any(kw in concepto_lower for kw in skip)

    def _is_skip_line(self, line):
        return any(kw in line.lower() for kw in self.EXCLUDE_KEYWORDS)

    def _parse_amount(self, s: str) -> float:
        if not s or s.strip() in ["", "-"]:
            return 0.0
        clean = s.replace("$", "").replace(" ", "").replace(".", "").replace(",", ".")
        neg = clean.startswith("-") or clean.endswith("-")
        clean = clean.replace("-", "")
        try:
            val = float(clean)
            return -val if neg else val
        except Exception:
            return 0.0
