import argparse
import logging
import pandas as pd
from pathlib import Path
from core.config import ConfigManager
from tables import TablesExtractor
from text import TextExtractor
from utils.logger import setup_logging


setup_logging()
log = logging.getLogger("pdf2xls_main")

def find_bank_for_text(full_text, configs):
    text_up = full_text.upper()
    for cfg in configs:
        for kw in cfg.get("keywords", []):
            if kw.upper() in text_up:
                return cfg.get("name")
    return None

def process_pdf(pdf_path: Path, out_dir: Path, cfg_manager: ConfigManager):
    log.info(f"Processing {pdf_path}")
    config = None
    # read text quick to try detect bank
    try:
        import pdfplumber
        with pdfplumber.open(str(pdf_path)) as doc:
            full_text = "\n".join(p.extract_text() or "" for p in doc.pages)
    except Exception:
        full_text = ""

    bank_name = find_bank_for_text(full_text, cfg_manager.load_all_configs())
    if bank_name:
        log.info(f"Detected bank: {bank_name}")
        config = cfg_manager.get(bank_name)
    else:
        config = cfg_manager.get("default")

    tables_ex = TablesExtractor(config)
    text_ex = TextExtractor(config)

    rows = []
    try:
        rows = tables_ex.extract_from_tables(str(pdf_path))
    except Exception as e:
        log.exception("tables extractor failed, will fallback to text", exc_info=e)

    if not rows:
        log.info("No table-based rows detected, using text fallback")
        rows = text_ex.extract_from_pdf(str(pdf_path))

    if not rows:
        log.warning("No rows extracted for file %s", pdf_path)
        return False

    df = pd.DataFrame(rows)
    # reorder columns
    preferred = ["fecha", "detalle", "referencia", "debitos", "creditos", "saldo"]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    out_file = out_dir / (pdf_path.stem + ".xlsx")
    df.to_excel(out_file, index=False)
    log.info(f"Exported to {out_file}")
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", "-i", default="../input", help="input folder with PDFs (relative to this file)")
    parser.add_argument("--output", "-o", default="../output", help="output folder for excels")
    parser.add_argument("--configs", "-c", default="../configs", help="folder with bank configs")
    args = parser.parse_args()

    base = Path(__file__).resolve().parent
    input_dir = (base / args.input).resolve()
    out_dir = (base / args.output).resolve()
    cfg_dir = (base / args.configs).resolve()

    input_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg_manager = ConfigManager(cfg_dir)
    pdfs = list(input_dir.glob("*.pdf"))
    if not pdfs:
        log.info("No PDFs found in input folder.")
        return

    for pdf in pdfs:
        try:
            process_pdf(pdf, out_dir, cfg_manager)
        except Exception:
            log.exception("Error processing %s", pdf)

if __name__ == "__main__":
    main()
