# -*- coding: utf-8 -*-
import pandas as pd
import re
import logging
from .base_parser import BaseParser
from utils.date_utils import normalize_date, extract_year_month

logger = logging.getLogger(__name__)

class NacionParser(BaseParser):
    """
    Parser específico para Banco de la Nación Argentina.
    Maneja multilínea, comprobante opcional y saldos inicial/final.
    """

    BANK_NAME = "NACION"
    DETECTION_KEYWORDS = ["BANCO DE LA NACION", "NACION", "SSI"]
    DATE_PATTERN = re.compile(r"(\d{2}/\d{2}/\d{2,4})")
    AMOUNT_PATTERN = re.compile(r"-?\$?\s*\d{1,3}(?:\.\d{3})*,\d{2}")

    def parse(self, raw_data, filename=""):
        logger.info(f"Procesando extracto Banco Nación: {filename}")
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
        """Parsea DataFrame de Camelot."""
        rows = []
        for idx, row in df.iterrows():
            try:
                if len(row) >= 4:
                    fecha_str = str(row[0]).strip()
                    concepto = str(row[1]).strip() if len(row) > 1 else ""
                    comprob = str(row[2]).strip() if len(row) > 2 else ""
                    debito_str = str(row[3]).strip() if len(row) > 3 else ""
                    credito_str = str(row[4]).strip() if len(row) > 4 else ""
                    saldo_str = str(row[5]).strip() if len(row) > 5 else ""

                    if self._is_valid_row(fecha_str, concepto):
                        parsed = self._build_row(
                            fecha_str, concepto, comprob,
                            debito_str, credito_str, saldo_str
                        )
                        if parsed:
                            rows.append(parsed)
            except Exception as e:
                logger.warning(f"[{self.BANK_NAME}] Error procesando fila {idx}: {e}")
                continue
        return rows

    def _parse_text_lines(self, lines):
        """Parsea líneas de texto plano con multilínea y comprobante opcional."""
        rows = []
        buffer_concept = []
        current_tx = None

        pattern = re.compile(
            r"(?P<fecha>\d{2}/\d{2}/\d{2,4})\s+"
            r"(?P<detalle>.+?)"
            r"(?:\s+(?P<comprob>\d+))?"
            r"\s+(?P<debito>[\d\.,-]+)?"
            r"\s+(?P<credito>[\d\.,-]+)?"
            r"\s+(?P<saldo>[\d\.,-]+)"
            r"$"
        )

        for line in lines:
            clean = line.strip()
            if not clean or self._is_skip_line(clean):
                continue

            match = pattern.search(clean)
            if match:
                if current_tx:
                    if buffer_concept:
                        current_tx["detalle"] += " " + " ".join(buffer_concept).strip()
                        buffer_concept = []
                    rows.append(current_tx)
                    current_tx = None

                fecha_str = match.group("fecha")
                detalle = match.group("detalle") or ""
                comprob = match.group("comprob") or ""
                debito_str = match.group("debito")
                credito_str = match.group("credito")
                saldo_str = match.group("saldo")

                current_tx = self._build_row(
                    fecha_str, detalle, comprob, debito_str, credito_str, saldo_str
                )
            else:
                # multilínea concepto
                if current_tx:
                    buffer_concept.append(clean)

        if current_tx:
            if buffer_concept:
                current_tx["detalle"] += " " + " ".join(buffer_concept).strip()
            rows.append(current_tx)

        return rows

    def _build_row(self, fecha_str, detalle, comprob, debito_str, credito_str, saldo_str):
        """Construye fila normalizada."""
        try:
            fecha_norm = normalize_date(fecha_str)
            if not fecha_norm:
                return None
            año, mes = extract_year_month(fecha_norm)
            return {
                "fecha": fecha_norm,
                "mes": mes,
                "año": año,
                "detalle": detalle[:200],
                "referencia": comprob[:50],
                "debito": self._to_amount(debito_str),
                "credito": self._to_amount(credito_str),
                "saldo": self._to_amount(saldo_str),
            }
        except Exception as e:
            logger.warning(f"[{self.BANK_NAME}] Error construyendo fila: {e}")
            return None

    def _is_valid_row(self, fecha_str, detalle):
        if not re.match(r"\d{2}/\d{2}/\d{2,4}", fecha_str):
            return False
        skip = ["fecha", "movimientos", "comprob", "débito", "crédito", "saldo"]
        return not any(kw in detalle.lower() for kw in skip)

    def _is_skip_line(self, line):
        skip = [
            "banco de la nacion", "resumen de cuenta", "cuenta corriente",
            "periodo", "hoja", "total grav", "saldo final", "saldo anterior"
        ]
        return any(kw in line.lower() for kw in skip)

    def _to_amount(self, raw: str) -> float:
        """Convierte string a float con formato AR."""
        if not raw or raw.strip() in ["", "-", "nan"]:
            return 0.0
        try:
            t = str(raw).strip().replace("$", "").replace(" ", "")
            neg = t.startswith("-") or t.endswith("-")
            t = t.replace("-", "").replace(".", "").replace(",", ".")
            v = float(t)
            return -v if neg else v
        except Exception:
            logger.warning(f"[{self.BANK_NAME}] No se pudo parsear monto: {raw}")
            return 0.0
