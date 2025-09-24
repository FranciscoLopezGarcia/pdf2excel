import logging
from extractors.ocr_extractor import OCRExtractor

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

PDF = r"input\ENERO 2025.pdf"

ocr = OCRExtractor(lang="spa")
pages = ocr.extract_text_pages(PDF, dpi_quick=160, dpi_full=300)

print("="*60)
if not pages:
    print("⚠️ No hubo páginas relevantes")
else:
    print(f"✅ Páginas relevantes: {[p for p,_ in pages]}")
    for p, txt in pages:
        print("-"*60)
        print(f"[Página {p}] primeros 800 chars:\n{txt[:800]}")
print("="*60)
