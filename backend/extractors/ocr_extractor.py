# extractors/ocr_extractor.py
import logging
import re
from typing import List, Tuple
from pdf2image import convert_from_path
import pytesseract
from PIL import Image, ImageOps

logger = logging.getLogger(__name__)


class OCRExtractor:
    def __init__(
        self,
        lang: str = "spa",
        tesseract_cmd: str = r"C:\Users\FranciscoLópezGarcía\AppData\Local\Programs\Tesseract-OCR\tesseract.exe",
        poppler_bin: str = r"C:\Users\FranciscoLópezGarcía\Downloads\Release-25.07.0-0 (2)\poppler-25.07.0\Library\bin",
    ):
        """
        OCR con Tesseract (Windows).
        - lang: idioma OCR (ej. 'spa')
        - tesseract_cmd: ruta al ejecutable de Tesseract
        - poppler_bin: carpeta /bin de Poppler para pdf2image
        """
        self.lang = lang
        self.poppler_bin = poppler_bin

        # Config de Tesseract
        self.tesseract_cmd = tesseract_cmd
        pytesseract.pytesseract.tesseract_cmd = self.tesseract_cmd
        self.tess_config = "--oem 1 --psm 6"  # psm 6: bloque uniforme de texto (mejor para tablas)

        # Palabras clave típicas de tablas de movimientos
        self.header_keywords = {
            "fecha", "concepto", "descripción", "descripcion", "detalle",
            "importe", "saldo", "débito", "debito", "crédito", "credito",
            "n°", "nro", "comprobante", "referencia"
        }

        # Patrones robustos para fechas (dd/mm/yyyy, dd-mm-yyyy, dd/mm/yy)
        self.re_fecha = re.compile(r"\b([0-3]?\d)[/-]([01]?\d)[/-](\d{2}|\d{4})\b")
        # Patrones de importes (estilo ES/AR y US)
        self.re_importe = re.compile(
            r"(?<!\w)(?:-?\$?\s?\d{1,3}(?:[.,]\d{3})*[.,]\d{2})(?!\w)"
        )

    # ---------- PREPROCESO DE IMAGEN ----------
    def _preprocess(self, img: Image.Image, scale: float = 1.35, thr: int = 180) -> Image.Image:
        """Mejora la imagen para OCR: escala, gris, autocontraste y binariza."""
        w, h = img.size
        if scale != 1.0:
            img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
        img = img.convert("L")
        img = ImageOps.autocontrast(img)
        # Binarizado simple
        img = img.point(lambda p: 255 if p > thr else 0)
        return img

    # ---------- DETECTOR DE RELEVANCIA ----------
    def _es_pagina_relevante(self, texto: str) -> Tuple[bool, dict]:
        """
        Heurística de relevancia:
        - headers: cuántos headers de tabla aparecen
        - fechas: cuántas fechas
        - importes: cuántos importes
        Reglas (ajustables):
          * (headers>=2 y (fechas>=5 o importes>=5))  ó
          * (fechas>=8 y importes>=6)                 ó
          * (headers>=3)
        """
        t = texto.lower()

        headers = sum(1 for k in self.header_keywords if k in t)
        fechas = len(self.re_fecha.findall(texto))
        importes = len(self.re_importe.findall(texto))

        relevant = (
            (headers >= 2 and (fechas >= 5 or importes >= 5)) or
            (fechas >= 8 and importes >= 6) or
            (headers >= 3)
        )
        return relevant, {"headers": headers, "fechas": fechas, "importes": importes}

    # ---------- OCR PÁGINA POR PÁGINA (2 PASOS) ----------
    def extract_text_pages(
        self,
        pdf_path: str,
        dpi_quick: int = 160,
        dpi_full: int = 300
    ) -> List[Tuple[int, str]]:
        """
        Devuelve [(page_num, texto)] SOLO de páginas relevantes.
        1) Pase rápido (dpi_quick) para detectar relevancia.
        2) Re-OCR de las páginas relevantes a dpi_full para calidad.
        """
        # PASO 1: OCR rápido para decidir relevancia
        logger.info(f"OCR quick pass (dpi={dpi_quick}) → {pdf_path}")
        try:
            imgs_quick = convert_from_path(pdf_path, dpi=dpi_quick, poppler_path=self.poppler_bin)
        except Exception as e:
            logger.error(f"convert_from_path (quick) falló: {e}")
            return []

        relevantes_idx = []
        for i, img in enumerate(imgs_quick, start=1):
            try:
                img_p = self._preprocess(img, scale=1.25, thr=180)
                raw = pytesseract.image_to_string(img_p, lang=self.lang, config=self.tess_config)
                text = raw.encode("latin-1", errors="ignore").decode("latin-1")
                is_rel, stats = self._es_pagina_relevante(text)
                logger.info(
                    f"Página {i} quick → relev={is_rel} | headers={stats['headers']} "
                    f"fechas={stats['fechas']} importes={stats['importes']} "
                    f"| chars={len(text)}"
                )
                if is_rel:
                    relevantes_idx.append(i)
            except Exception as e:
                logger.error(f"OCR quick falló en página {i}: {e}")

        if not relevantes_idx:
            logger.warning("⚠️ Ninguna página calificada como relevante en el quick pass.")
            return []

        # PASO 2: Re-OCR de esas páginas en alta calidad
        logger.info(f"OCR full pass (dpi={dpi_full}) solo en páginas {relevantes_idx}")
        try:
            imgs_full = convert_from_path(pdf_path, dpi=dpi_full, poppler_path=self.poppler_bin)
        except Exception as e:
            logger.error(f"convert_from_path (full) falló: {e}")
            return []

        resultados: List[Tuple[int, str]] = []
        for i in relevantes_idx:
            try:
                img = imgs_full[i - 1]  # mismo índice de página
                img_p = self._preprocess(img, scale=1.35, thr=180)
                raw = pytesseract.image_to_string(img_p, lang=self.lang, config=self.tess_config)
                text = raw.encode("latin-1", errors="ignore").decode("latin-1")
                resultados.append((i, text))
                logger.info(f"Página {i} full → {len(text)} chars")
            except Exception as e:
                logger.error(f"OCR full falló en página {i}: {e}")

        return resultados

    # ---------- Compatibilidad con la lógica anterior ----------
    def extract_text_from_pdf(self, pdf_path: str) -> str:
        """
        Mantiene el nombre esperado por el pipeline viejo.
        Concatena únicamente las páginas relevantes.
        """
        pages = self.extract_text_pages(pdf_path)
        return "\n\n".join([f"--- Página {p} ---\n{t}" for p, t in pages])

    def extract_text(self, pdf_path: str, dpi_quick=160, dpi_full=300) -> str:
        """
        Método usado en tus tests previos. Devuelve concatenado.
        """
        pages = self.extract_text_pages(pdf_path, dpi_quick=dpi_quick, dpi_full=dpi_full)
        return "\n\n".join([f"--- Página {p} ---\n{t}" for p, t in pages])
