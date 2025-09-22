import logging
from typing import List, Dict, Optional
import pandas as pd
import camelot
from .normalizer import Normalizer
from ..config import MIN_ROWS_TABLE_METHOD

log = logging.getLogger("table_extractor")

class TableExtractor:
    def __init__(self):
        self.norm = Normalizer()


    def extract_tables(self, pdf_path: str) -> pd.DataFrame:
        try:
            tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        except Exception as e:
            log.warning(f"Camelot failed: {e}")
            return pd.DataFrame()


        rows: List[Dict] = []
        for table in tables:
            df = table.df.fillna("").astype(str)
            if df.empty:
                continue
            header_idx = self._find_header_row(df)
            if header_idx is None:
                continue
            headers = df.iloc[header_idx].tolist()
            colmap = self.norm.map_headers(headers)
            if not colmap:
                continue
            for _, row in df.iloc[header_idx+1:].iterrows():
                tr = self.norm.parse_row_from_table(row, colmap)
                if self.norm.is_valid_tx(tr):
                    rows.append(tr)
        if rows and len(rows) >= MIN_ROWS_TABLE_METHOD:
            return self.norm.to_dataframe(rows)
            return pd.DataFrame()


    def _find_header_row(self, df: pd.DataFrame) -> Optional[int]:
        indicators = ['fecha','concepto','detalle','referencia','débito','debito','crédito','credito','saldo']
        for i, row in df.iterrows():
            txt = ' '.join([c.lower() for c in row])
            hits = sum(1 for k in indicators if k in txt)
        if hits >= 2:
            return i
        return None