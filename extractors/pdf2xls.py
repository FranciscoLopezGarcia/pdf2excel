#!/usr/bin/env python3
"""
Simple PDF to Excel Bank Statement Extractor
Drop PDFs in input folder, get Excel files in output folder.
No configuration needed.
"""

import os
import sys
from pathlib import Path
import logging
from .universal_extractor import UniversalBankExtractor

# Setup simple logging
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
    # Setup paths - input is outside extractors folder
    script_dir = Path(__file__).parent
    project_root = script_dir.parent  # Go up one level from extractors
    input_dir = project_root / "input"
    output_dir = project_root / "output"
    
    # Create directories
    input_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)
    
    log.info(f"Input directory: {input_dir}")
    log.info(f"Output directory: {output_dir}")
    
    # Find PDF files recursively in all subdirectories
    pdf_files = list(input_dir.rglob("*.pdf"))
    if not pdf_files:
        log.warning(f"No PDF files found in {input_dir} (searched recursively)")
        print(f"\nNo PDF files found in {input_dir} or subdirectories")
        print("Please add PDF files to the input folder and run again.")
        return
    
    log.info(f"Found {len(pdf_files)} PDF files to process")
    
    # Initialize extractor
    extractor = UniversalBankExtractor()
    
    # Process each PDF
    processed = 0
    failed = 0
    
    for pdf_file in pdf_files:
        try:
            # Create relative path for output structure
            rel_path = pdf_file.relative_to(input_dir)
            output_subdir = output_dir / rel_path.parent
            output_subdir.mkdir(parents=True, exist_ok=True)
            
            log.info(f"Processing: {rel_path}")
            print(f"\nProcessing: {rel_path}")
            
            # Extract data
            df = extractor.extract_from_pdf(str(pdf_file))
            
            if df.empty:
                log.warning(f"No data extracted from {rel_path}")
                print(f"  ⚠️  No transactions found")
                failed += 1
                continue
            
            # Generate output filename (preserve folder structure)
            output_file = output_subdir / f"{pdf_file.stem}.xlsx"
            
            # Save to Excel
            df.to_excel(output_file, index=False, sheet_name="Transactions")
            
            log.info(f"Saved {len(df)} transactions to {output_file.relative_to(output_dir)}")
            print(f"  ✅ Extracted {len(df)} transactions → {output_file.relative_to(output_dir)}")
            processed += 1
            
        except Exception as e:
            log.error(f"Failed to process {pdf_file.relative_to(input_dir)}: {e}")
            print(f"  ❌ Failed: {e}")
            failed += 1
    
    # Summary
    log.info(f"Processing complete: {processed} successful, {failed} failed")
    print(f"\n" + "="*50)
    print(f"SUMMARY:")
    print(f"  Processed: {processed} files")
    print(f"  Failed: {failed} files")
    print(f"  Output folder: {output_dir}")
    
    if processed > 0:
        print(f"\n✅ Check the output folder for your Excel files!")
    
    # Wait for user input before closing (useful when double-clicking)
    if len(sys.argv) == 1:  # No command line arguments = probably double-clicked
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