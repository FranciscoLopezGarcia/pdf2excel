# -*- coding: utf-8 -*-
import re
import logging
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class BBVAParser:
    """
    Parser específico para Banco BBVA.
    Detecta movimientos con formato dd/mm o dd/mm/yyyy
    Montos en formato argentino (1.234,56) con signos opcionales.
    """

    BANK_NAME = "BBVA"

    DATE_PATTERN = re.compile(r"(\d{1,2}[/-]\d{1,2})(?:[/-](\d{2,4}))?")
    AMOUNT_PATTERN = re.compile(r"-?\$?\s*\d{1,3}(?:\.\d{3})*,\d{2}")

    EXCLUDE_KEYWORDS = [
        "movimientos en cuentas", "total movimientos", "saldo del período",
        "saldo al", "página", "fecha", "concepto", "débito", "crédito"
    ]

    def detect(self, text: str, filename: str = "") -> bool:
        return "BBVA" in (text.upper() + filename.upper())

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        logger.info(f"Procesando extracto {self.BANK_NAME}: {filename}")

        if isinstance(raw_data, str):
            lines = raw_data.splitlines()
        elif isinstance(raw_data, list):
            lines = raw_data
        else:
            return pd.DataFrame(columns=self.required_columns())

        rows = []
        year_hint = self._infer_year(raw_data, filename)

        current_tx = None
        buffer_concept = []

        for line in lines:
            clean = line.strip()
            if not clean:
                continue
            if any(kw in clean.lower() for kw in self.EXCLUDE_KEYWORDS):
                continue

            date_match = self.DATE_PATTERN.match(clean)
            if date_match:
                if current_tx:
                    if buffer_concept:
                        current_tx["detalle"] = " ".join(buffer_concept).strip()
                        buffer_concept = []
                    rows.append(current_tx)
                    current_tx = None

                day_month, year = date_match.group(1), date_match.group(2)
                if year:
                    if len(year) == 2:
                        year = 2000 + int(year)
                    else:
                        year = int(year)
                else:
                    year = year_hint or datetime.now().year

                try:
                    fecha = datetime.strptime(f"{day_month}/{year}", "%d/%m/%Y").strftime("%d/%m/%Y")
                except Exception:
                    fecha = f"{day_month}/{year}"

                current_tx = {
                    "fecha": fecha,
                    "mes": str(datetime.strptime(fecha, "%d/%m/%Y").month).zfill(2) if "/" in fecha else "",
                    "año": str(datetime.strptime(fecha, "%d/%m/%Y").year) if "/" in fecha else "",
                    "detalle": "",
                    "referencia": "",
                    "debito": 0.0,
                    "credito": 0.0,
                    "saldo": 0.0,
                }
                continue

            if self.AMOUNT_PATTERN.match(clean):
                if current_tx:
                    amount = self._parse_amount(clean)
                    if current_tx["debito"] == 0.0 and clean.startswith("-"):
                        current_tx["debito"] = abs(amount)
                    elif current_tx["credito"] == 0.0 and not clean.startswith("-"):
                        current_tx["credito"] = amount
                    else:
                        current_tx["saldo"] = amount
                continue

            if current_tx:
                buffer_concept.append(clean)

        if current_tx:
            if buffer_concept:
                current_tx["detalle"] = " ".join(buffer_concept).strip()
            rows.append(current_tx)

        df = pd.DataFrame(rows, columns=self.required_columns())
        return df

    def _parse_amount(self, s: str) -> float:
        if not s or s.strip() in ["", "-"]:
            return 0.0
        t = s.replace("$", "").replace(" ", "").strip()
        neg = t.startswith("-") or t.endswith("-")
        t = t.replace("-", "").replace(".", "").replace(",", ".")
        try:
            v = float(t)
            return -v if neg else v
        except Exception:
            logger.warning(f"[{self.BANK_NAME}] No se pudo parsear monto: {s}")
            return 0.0

    def _infer_year(self, text: str, filename: str = "") -> int:
        match = re.search(r"(20\d{2}|19\d{2})", text)
        if match:
            return int(match.group(1))
        match = re.search(r"(20\d{2}|19\d{2})", filename)
        if match:
            return int(match.group(1))
        return datetime.now().year

    @staticmethod
    def required_columns():
        return ["fecha", "mes", "año", "detalle", "referencia", "debito", "credito", "saldo"]
