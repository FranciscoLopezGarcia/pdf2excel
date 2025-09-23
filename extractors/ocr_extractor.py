import os
import logging
from pathlib import Path
from pdf2image import convert_from_path
from paddleocr import PaddleOCR

log = logging.getLogger("ocr_extractor")

class OCRExtractor:
    def __init__(self, lang="es"):
        """
        lang: idioma principal ('es' para español, 'en' para inglés, etc.)
        """
        self.lang = lang
        # Inicializamos PaddleOCR solo una vez
        self.ocr = PaddleOCR(use_angle_cls=True, lang=lang)

    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Convierte PDF a imágenes y extrae texto con PaddleOCR.
        Devuelve todo el texto concatenado.
        """
        text_content = []
        try:
            log.info(f"OCR: convirtiendo {pdf_path} a imágenes...")
            pages = convert_from_path(pdf_path, dpi=300)
            log.info(f"OCR: {len(pages)} páginas convertidas")

            for idx, page in enumerate(pages, start=1):
                tmp_image = f"page_{idx}.png"
                page.save(tmp_image, "PNG")

                try:
                    result = self.ocr.ocr(tmp_image, cls=True)
                    page_text = ""
                    for line in result[0]:
                        page_text += line[1][0] + "\n"
                    text_content.append(page_text)
                    log.info(f"OCR: procesada página {idx} ({len(page_text)} caracteres)")
                except Exception as e:
                    log.warning(f"OCR falló en página {idx}: {e}")
                finally:
                    if os.path.exists(tmp_image):
                        os.remove(tmp_image)

        except Exception as e:
            log.error(f"OCR con PaddleOCR falló: {e}")
            return ""

        return "\n".join(text_content)
