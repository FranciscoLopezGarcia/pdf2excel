import logging
import pandas as pd
from ..extractor.universal_extractor import UniversalBankExtractor

log = logging.getLogger("router")


class BankRouter:
    """Router genérico: delega todo al UniversalBankExtractor."""

    def __init__(self):
        self.extractor = UniversalBankExtractor()

    def extract(self, pdf_path: str) -> tuple[pd.DataFrame, bool]:
        try:
            return self.extractor.extract(pdf_path)
        except Exception as e:
            log.exception(f"Extraction failed for {pdf_path}: {e}")
            return pd.DataFrame(), False
