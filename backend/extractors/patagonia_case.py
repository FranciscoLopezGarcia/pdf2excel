# extractors/patagonia_case.py
import logging
import pandas as pd
import pdfplumber
import re

log = logging.getLogger("patagonia_case")

class PatagoniaExtractor:
    def __init__(self):
        # regex comunes (fechas y montos)
        self.re_fecha = re.compile(r"\b\d{1,2}[/-]\d{1,2}[/-]\d{2,4}\b")
        self.re_importe = re.compile(r"-?\d{1,3}(?:[.,]\d{3})*[.,]\d{2}")

    def extract(self, pdf_path: str) -> pd.DataFrame:
        log.info(f"Procesando caso especial Patagonia → {pdf_path}")
        transactions = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text() or ""
                lines = [l.strip() for l in text.splitlines() if l.strip()]

                for line in lines:
                    if self.re_fecha.search(line) and self.re_importe.search(line):
                        parts = line.split()
                        fecha = parts[0]
                        detalle = " ".join(parts[1:-1])
                        importe = parts[-1]

                        transactions.append({
                            "fecha": fecha,
                            "detalle": detalle,
                            "referencia": "",
                            "debitos": importe if "-" in importe else "",
                            "creditos": importe if "-" not in importe else "",
                            "saldo": ""
                        })

        if not transactions:
            log.warning("⚠️ No se detectaron movimientos en Patagonia")

        return pd.DataFrame(transactions)
