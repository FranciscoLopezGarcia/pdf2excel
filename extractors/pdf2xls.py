"""
PDF → Excel Bank Statement Extractor (con BankRouter)
Drop PDFs en input/, obtén Excel en output/.
"""

import os
import sys
from pathlib import Path
import logging
import pandas as pd

from bank_router import BankRouter   # 🔹 ahora usamos el router

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pdf2xls.log')
    ]
)

log = logging.getLogger("pdf2xls")

def main():
    # Paths
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    input_dir = project_root / "input"
    output_dir = project_root / "output"

    # Crear carpetas si no existen
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    log.info(f"Input directory: {input_dir}")
    log.info(f"Output directory: {output_dir}")

    # Buscar PDFs recursivamente
    pdf_files = list(input_dir.rglob("*.pdf"))
    if not pdf_files:
        log.warning(f"No PDF files found in {input_dir} (searched recursively)")
        print(f"\nNo PDF files found in {input_dir} or subdirectories")
        print("Please add PDF files to the input folder and run again.")
        return

    log.info(f"Found {len(pdf_files)} PDF files to process")

    # 🔹 Inicializar router
    router = BankRouter()

    processed = 0
    failed = 0

    for pdf_file in pdf_files:
        try:
            rel_path = pdf_file.relative_to(input_dir)
            output_subdir = output_dir / rel_path.parent
            output_subdir.mkdir(parents=True, exist_ok=True)

            log.info(f"Processing: {rel_path}")
            print(f"\nProcessing: {rel_path}")

            # 🔹 Extraer con BankRouter
            df = router.extract(str(pdf_file))

            if df.empty:
                log.warning(f"No data extracted from {rel_path}")
                print("  ⚠️  No transactions found")
                failed += 1
                continue

            # Guardar Excel
            output_file = output_subdir / f"{pdf_file.stem}.xlsx"
            df.to_excel(output_file, index=False, sheet_name="Transactions")

            log.info(f"Saved {len(df)} transactions to {output_file.relative_to(output_dir)}")
            print(f"  ✅ Extracted {len(df)} transactions → {output_file.relative_to(output_dir)}")
            processed += 1

        except Exception as e:
            log.error(f"Failed to process {pdf_file.relative_to(input_dir)}: {e}")
            print(f"  ❌ Failed: {e}")
            failed += 1

    # Resumen
    log.info(f"Processing complete: {processed} successful, {failed} failed")
    print("\n" + "="*50)
    print("SUMMARY:")
    print(f"  Processed: {processed} files")
    print(f"  Failed: {failed} files")
    print(f"  Output folder: {output_dir}")

    if processed > 0:
        print("\n✅ Check the output folder for your Excel files!")

    # Esperar input antes de cerrar (si fue doble click)
    if len(sys.argv) == 1:
        input("\nPress Enter to exit...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nCancelled by user")
    except Exception as e:
        log.error(f"Unexpected error: {e}")
        print(f"\n❌ Unexpected error: {e}")
        input("Press Enter to exit...")
