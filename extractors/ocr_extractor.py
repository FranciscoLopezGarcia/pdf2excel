import os
import tempfile
import pytesseract
from pdf2image import convert_from_path
import logging
from config import POPPLER_PATH, TESSERACT_PATH

pytesseract.pytesseract.tesseract_cmd = TESSERACT_PATH


log = logging.getLogger("ocr_extractor")

class OCRExtractor:
    def __init__(self, lang="spa"):
        """
        OCR Extractor usando Tesseract en lugar de PaddleOCR.
        :param lang: idioma (por defecto español 'spa').
        """
        self.lang = lang

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Convierte PDF en imágenes y extrae texto con Tesseract.
        """
        text_content = []
        try:
            # Convertir PDF a imágenes temporales
            images = convert_from_path(pdf_path, dpi=300, poppler_path=POPPLER_PATH)

            log.info(f"OCR: convertido {len(images)} páginas a imágenes")

            for i, img in enumerate(images):
                page_text = pytesseract.image_to_string(img, lang=self.lang, config="--oem 3 --psm 6")
                page_text = page_text.encode("latin-1", errors="ignore").decode("utf-8", errors="ignore")


                if page_text.strip():
                    text_content.append(page_text)
                else:
                    log.warning(f"OCR: página {i+1} vacía")

        except Exception as e:
            log.error(f"OCR con Tesseract falló: {e}")
            return ""

        return "\n".join(text_content)
