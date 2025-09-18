# extractors/tables_extractor.py
import camelot
import pandas as pd
import re
from utils.cleaner import clean_text, normalize_date, parse_amount, format_amount

EXCLUDE_KEYWORDS = ["estado de cuenta", "saldo anterior", "movimientos pendientes", "descarga"]

COLUMN_MAP_STANDARD = {
    "fecha": ["fecha", "fec", "dia"],
    "concepto": ["detalle", "concepto", "descripcion", "operacion", "causal"],
    "referencia": ["referencia", "ref", "nro"],
    "debito": ["debito", "debitos", "cargo"],
    "credito": ["credito", "creditos", "abono"],
    "saldo": ["saldo", "balance"]
}

class TablesExtractor:
    def __init__(self, field_mappings=None):
        self.field_mappings = field_mappings or COLUMN_MAP_STANDARD

    # ---------------- utils ----------------
    # def _clean_table(self, df: pd.DataFrame) -> pd.DataFrame:
    #     df = df.fillna("").astype(str).applymap(clean_text)
    #     # eliminar filas vacías
    #     df = df[~df.apply(lambda r: all(v.strip() == "" for v in r), axis=1)]
    #     # eliminar columnas vacías
    #     df = df.loc[:, ~(df.apply(lambda c: all(v.strip() == "" for v in c)))]
    #     return df

    def _detect_header_row(self, df: pd.DataFrame):
        for i, row in df.iterrows():
            row_lower = " ".join([str(x).lower() for x in row])
            if any(k in row_lower for k in ["fecha", "detalle", "saldo"]):
                return i
        return 0

    def _map_headers(self, headers):
        mapping = {}
        for i, h in enumerate(headers):
            h_lower = h.lower()
            for std, aliases in self.field_mappings.items():
                if any(a in h_lower for a in aliases):
                    mapping[i] = std
        return mapping

    def _parse_row(self, row, header_map, mode="auto"):
        parsed = {k: "" for k in ["fecha", "concepto", "referencia", "debito", "credito", "saldo"]}
        for idx, val in enumerate(row):
            field = header_map.get(idx)
            if not field:
                continue
            val = str(val).strip()
            if field == "fecha":
                parsed["fecha"] = normalize_date(val)
            elif field == "concepto":
                parsed["concepto"] = val
            elif field == "referencia":
                parsed["referencia"] = val
            elif field in ["debito", "credito", "saldo", "debitos", "creditos"]:
                num = parse_amount(val)
                if field == "saldo":
                    parsed["saldo"] = format_amount(num)
                elif field == "debito" or field == "debitos":
                    parsed["debito"] = format_amount(num)
                elif field == "credito" or field == "creditos":
                    parsed["credito"] = format_amount(num)
                    
        # si solo hay una columna de importe y no hay debito/credito
        if mode == "auto" and parsed["debito"] == "" and parsed["credito"] == "" and "importe" in row.index:
            val = parse_amount(row["importe"])
            if val < 0:
                parsed["debito"] = format_amount(abs(val))
            else:
                parsed["credito"] = format_amount(val)
        return parsed

    # ---------------- main ----------------
    def extract_from_pdf(self, pdf_path: str):
        tables = camelot.read_pdf(pdf_path, flavor="stream", pages="all")
        results = []
        for t in tables:
            df = t.df
            df = self._clean_table(df)
            if df.empty:
                continue

            header_row_idx = self._detect_header_row(df)
            headers = df.iloc[header_row_idx].tolist()
            header_map = self._map_headers(headers)
            for _, row in df.iloc[header_row_idx + 1:].iterrows():
                parsed = self._parse_row(row, header_map)
                # filtrar filas vacías
                if any([parsed[k] for k in ["fecha", "debito", "credito", "saldo"]]):
                    results.append(parsed)
        return results
