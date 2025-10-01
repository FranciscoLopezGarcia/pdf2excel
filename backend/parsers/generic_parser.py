import pandas as pd
import logging
from .base_parser import BaseParser

logger = logging.getLogger(__name__)

class GenericParser(BaseParser):
    """
    Parser genérico: último recurso.
    Intenta usar el texto crudo sin reglas específicas.
    """

    def parse(self, raw_data, filename="") -> pd.DataFrame:
        logger.info(f"Usando GenericParser para {filename}")

        if not raw_data:
            logger.warning("⚠️ raw_data vacío en GenericParser")
            return pd.DataFrame(columns=self.REQUIRED_COLUMNS)

        # Caso 1: string plano (pdfplumber / OCR / Camelot serializado)
        if isinstance(raw_data, str):
            return self._from_text(raw_data.splitlines())

        # Caso 2: lista de DataFrames (Camelot viejo)
        if isinstance(raw_data, list) and hasattr(raw_data[0], "columns"):
            df = raw_data[0].copy()
            logger.info("GenericParser: usando DataFrame de Camelot")
            return self._from_dataframe(df)

        # Caso 3: lista de strings (compatibilidad)
        if isinstance(raw_data, list) and isinstance(raw_data[0], str):
            return self._from_text(raw_data)

        logger.warning("GenericParser: formato raw_data desconocido")
        return pd.DataFrame(columns=self.REQUIRED_COLUMNS)

    # ---------- Implementaciones auxiliares ----------

    def _from_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convierte DataFrame crudo en DataFrame estándar."""
        out = pd.DataFrame()
        out["fecha"] = df.iloc[:, 0].astype(str).apply(self.normalize_date)
        out["detalle"] = df.iloc[:, 1].astype(str).str.strip()
        if df.shape[1] >= 4:
            out["debito"] = pd.to_numeric(df.iloc[:, -3], errors="coerce").fillna(0)
            out["credito"] = pd.to_numeric(df.iloc[:, -2], errors="coerce").fillna(0)
            out["saldo"] = pd.to_numeric(df.iloc[:, -1], errors="coerce").fillna(0)
        else:
            out["debito"] = 0
            out["credito"] = 0
            out["saldo"] = 0
        out["referencia"] = ""
        return self.finalize(out)

    def _from_text(self, lines: list) -> pd.DataFrame:
        """Convierte texto plano en DataFrame estándar."""
        rows = []
        for line in lines:
            parts = line.split()
            if len(parts) >= 2 and ("/" in parts[0] or "-" in parts[0]):
                rows.append({
                    "fecha": self.normalize_date(parts[0]),
                    "detalle": " ".join(parts[1:]),
                    "debito": 0,
                    "credito": 0,
                    "saldo": 0,
                    "referencia": ""
                })
        df = pd.DataFrame(rows)
        return self.finalize(df)
