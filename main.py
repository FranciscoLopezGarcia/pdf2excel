import logging, sys
from extractors.pdf2xls import run_batch
from extractors.config import REPORTS_DIR, LOG_FILE

def setup_logging():
    REPORTS_DIR.mkdir(exist_ok=True, parents=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, encoding="utf-8")]
    )

def main():
    setup_logging()
    log = logging.getLogger("main")
    log.info("Batch start")
    report_df = run_batch()
    # Guardar reporte
    rep_xlsx = REPORTS_DIR / "batch_report.xlsx"
    rep_csv  = REPORTS_DIR / "batch_report.csv"
    report_df.to_excel(rep_xlsx, index=False)
    report_df.to_csv(rep_csv, index=False, encoding="utf-8-sig")
    log.info(f"Reporte guardado: {rep_xlsx}")
    # Resumen consola
    ok = (report_df["status"]=="OK").sum()
    empty = (report_df["status"]=="EMPTY").sum()
    errs = report_df["status"].str.startswith("ERROR").sum()
    ocr = (report_df["ocr_used"]==True).sum()
    print("\n===== SUMMARY =====")
    print(f"OK: {ok} | EMPTY: {empty} | ERRORS: {errs} | OCR_USED: {ocr}")
    print(f"Reporte: {rep_xlsx}")

if __name__ == "__main__":
    main()
