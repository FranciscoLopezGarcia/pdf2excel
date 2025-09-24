import logging
import os
from extractors.ocr_extractor import OCRExtractor

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(message)s")

PDF = r"input\ENERO 2025.pdf"
OUTPUT_DIR = "output_txt"
os.makedirs(OUTPUT_DIR, exist_ok=True)

ocr = OCRExtractor(lang="spa")

# Extraemos texto de páginas relevantes
pages = ocr.extract_text_pages(PDF, dpi_quick=160, dpi_full=300)

if not pages:
    print("⚠️ No se detectaron páginas relevantes")
else:
    print(f"✅ Detectadas {len(pages)} páginas relevantes")
    for page_num, text in pages:
        file_path = os.path.join(OUTPUT_DIR, f"page_{page_num}.txt")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(text)
        print(f"💾 Guardado OCR completo de página {page_num} → {file_path}")

print("=" * 60)
print("Listo. Revisá los .txt en la carpeta:", OUTPUT_DIR)
