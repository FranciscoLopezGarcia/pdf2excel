import pandas as pd
import logging
import re
from .base_parser import BaseParser

logger = logging.getLogger(__name__)


class SupervielleUSDParser(BaseParser):
    """
    Parser específico para Banco Supervielle (resúmenes en USD).
    Estandariza las columnas al formato requerido.
    """

    BANK_NAME = "SUPERVIELLE_USD"
    PREFER_TABLES = True
    DETECTION_KEYWORDS = ["SUPERVIELLE USD", "SUPERVIELLE EN DOLARES", "SUPERVIELLE RESUMEN USD"]

    def detect(self, text: str, filename: str = "") -> bool:
        haystack = f"{text} {filename}".upper()
        return "SUPERVIELLE" in haystack and (" USD" in haystack or "USD" in haystack or "DOLARES" in haystack)

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        logger.info(f"Procesando {self.BANK_NAME} - {filename}")

        if isinstance(raw_data, str):
            return self._parse_text_lines(raw_data.splitlines())
        elif isinstance(raw_data, list) and len(raw_data) > 0 and hasattr(raw_data[0], "columns"):
            return self._parse_dataframe(raw_data[0])
        elif isinstance(raw_data, list) and all(isinstance(x, str) for x in raw_data):
            return self._parse_text_lines(raw_data)
        else:
            logger.warning("Formato raw_data no reconocido en SupervielleUSDParser")
            return pd.DataFrame(columns=self.REQUIRED_COLUMNS)

    # ----------------- Implementaciones internas -----------------

    def _parse_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte un DataFrame crudo en formato estándar."""
        out = pd.DataFrame()
        try:
            out["fecha"] = df.iloc[:, 0].astype(str)
            out["detalle"] = df.iloc[:, 1].astype(str)
            if df.shape[1] >= 4:
                out["debito"] = pd.to_numeric(df.iloc[:, -3], errors="coerce").fillna(0.0)
                out["credito"] = pd.to_numeric(df.iloc[:, -2], errors="coerce").fillna(0.0)
                out["saldo"] = pd.to_numeric(df.iloc[:, -1], errors="coerce").fillna(0.0)
            else:
                out["debito"] = 0.0
                out["credito"] = 0.0
                out["saldo"] = 0.0
            out["referencia"] = ""
        except Exception as e:
            logger.error(f"Error parseando DataFrame Supervielle: {e}", exc_info=True)
            return pd.DataFrame(columns=self.REQUIRED_COLUMNS)

        return self.finalize(out)

    def _parse_text_lines(self, lines: list) -> pd.DataFrame:
        """Convierte texto en DataFrame estándar (fallback si no hay tablas)."""
        rows = []
        regex = re.compile(
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
        df = pd.DataFrame(rows)
        return self.finalize(df)

    # ----------------- Utils -----------------

    def _to_amount(self, val) -> float:
        if not val:
            return 0.0
        try:
            return float(str(val).replace(".", "").replace(",", "."))
        except Exception:
            return 0.0
