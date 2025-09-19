import logging
from pathlib import Path
import pandas as pd
from .config import INPUT_DIR, OUTPUT_DIR
from .bank_router import BankRouter

log = logging.getLogger("pdf2xls")

def run_batch() -> pd.DataFrame:
    """Procesa TODOS los PDFs en input/ y guarda Excels en output/; devuelve dataframe reporte."""
    router = BankRouter()
    pdf_files = list(INPUT_DIR.rglob("*.pdf"))
    if not pdf_files:
        log.warning(f"No se encontraron PDFs en {INPUT_DIR}")
        return pd.DataFrame(columns=["file","rows","ocr_used","status","output"])

    records=[]
    for pdf in pdf_files:
        rel = pdf.relative_to(INPUT_DIR)
        out_dir = (OUTPUT_DIR / rel.parent)
        out_dir.mkdir(parents=True, exist_ok=True)
        log.info(f"Processing: {rel}")
        try:
            df, ocr_used = router.extract(str(pdf))
            if df.empty:
                records.append({"file": str(rel), "rows": 0, "ocr_used": ocr_used, "status":"EMPTY", "output": ""})
                continue

            out_file = out_dir / f"{pdf.stem}.xlsx"
            df.to_excel(out_file, index=False, sheet_name="Transactions")
            records.append({"file": str(rel), "rows": len(df), "ocr_used": ocr_used, "status":"OK", "output": str(out_file.relative_to(OUTPUT_DIR))})
            log.info(f"Saved {len(df)} rows → {out_file}")
        except Exception as e:
            log.error(f"Failed {rel}: {e}")
            records.append({"file": str(rel), "rows": 0, "ocr_used": False, "status":f"ERROR: {e}", "output": ""})

    return pd.DataFrame.from_records(records)
