from pathlib import Path
import pandas as pd
import logging
from ..config import INPUT_DIR, OUTPUT_DIR
from ..router.bank_router import BankRouter

log = logging.getLogger("batch")


class BatchProcessor:
    def __init__(self):
        self.router = BankRouter()

    def _infer_bank_name(self, pdf_path: Path) -> str:
        """Heurística simple: usa el nombre del archivo sin extensión como banco"""
        return pdf_path.stem

    def run(self) -> pd.DataFrame:
        pdf_files = list(INPUT_DIR.rglob("*.pdf"))
        if not pdf_files:
            log.warning(f"No se encontraron PDFs en {INPUT_DIR}")
            return pd.DataFrame(columns=["file", "banco", "rows", "ocr_used", "status", "output"])

        records = []
        unificado = []

        for pdf in pdf_files:
            rel = pdf.relative_to(INPUT_DIR)
            out_dir = OUTPUT_DIR / rel.parent
            out_dir.mkdir(parents=True, exist_ok=True)
            log.info(f"Processing: {rel}")

            try:
                df, ocr_used = self.router.extract(str(pdf))
                banco = self._infer_bank_name(pdf)

                if df.empty:
                    # Guardar registro aunque esté vacío
                    records.append({
                        "file": str(rel),
                        "banco": banco,
                        "rows": 0,
                        "ocr_used": ocr_used,
                        "status": "EMPTY",
                        "output": ""
                    })
                else:
                    # Columna banco para unificado
                    df_unificado = df.copy()
                    df_unificado["banco"] = banco
                    unificado.append(df_unificado)

                    # Guardar individual
                    out_file = out_dir / f"{pdf.stem}.xlsx"
                    df.to_excel(out_file, index=False, sheet_name="Transactions")

                    records.append({
                        "file": str(rel),
                        "banco": banco,
                        "rows": len(df),
                        "ocr_used": ocr_used,
                        "status": "OK",
                        "output": str(out_file.relative_to(OUTPUT_DIR))
                    })
                    log.info(f"Saved {len(df)} rows → {out_file}")

            except Exception as e:
                log.exception(f"Failed {rel}: {e}")
                records.append({
                    "file": str(rel),
                    "banco": "",
                    "rows": 0,
                    "ocr_used": False,
                    "status": f"ERROR: {e}",
                    "output": ""
                })

        # UNIFICADO
        if unificado:
            dfu = pd.concat(unificado, ignore_index=True)

            # ordenar columnas si existen
            cols = [c for c in ["fecha", "detalle", "referencia", "debito", "credito", "saldo", "banco"] if c in dfu.columns]
            dfu = dfu[cols + [c for c in dfu.columns if c not in cols]]

            unificado_path = OUTPUT_DIR / "UNIFICADO.xlsx"
            unificado_path.parent.mkdir(parents=True, exist_ok=True)
            dfu.to_excel(unificado_path, index=False)
            log.info(f"UNIFICADO guardado → {unificado_path} ({len(dfu)} filas)")

        return pd.DataFrame.from_records(records)
