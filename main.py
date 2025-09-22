import pandas as pd
from logging import getLogger
from logging import INFO
from .logging_utils import setup_logging
from .batch.batch_process import BatchProcessor
from .config import REPORTS_DIR

if __name__ == "__main__":
    setup_logging(INFO)
    log = getLogger("main")
    log.info("Batch start")


    REPORTS_DIR.mkdir(parents=True, exist_ok=True)


    bp = BatchProcessor()
    report_df = bp.run()


# Guardar reporte
    rep_xlsx = REPORTS_DIR / "batch_report.xlsx"
    rep_csv = REPORTS_DIR / "batch_report.csv"
    report_df.to_excel(rep_xlsx, index=False)
    report_df.to_csv(rep_csv, index=False, encoding="utf-8-sig")
    log.info(f"Reporte guardado: {rep_xlsx}")


# Resumen consola
    ok = (report_df["status"]=="OK").sum() if not report_df.empty else 0
    empty = (report_df["status"]=="EMPTY").sum() if not report_df.empty else 0
    errs = report_df["status"].astype(str).str.startswith("ERROR").sum() if not report_df.empty else 0
    ocr = (report_df["ocr_used"]==True).sum() if not report_df.empty else 0


    print("\n===== SUMMARY =====")
    print(f"OK: {ok} | EMPTY: {empty} | ERRORS: {errs} | OCR_USED: {ocr}")
    print(f"Reporte: {rep_xlsx}")