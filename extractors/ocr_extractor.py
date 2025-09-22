# ocr_extractor.py
import os
import tempfile
from paddleocr import PaddleOCR
from pdf2image import convert_from_path

# Inicializar OCR en CPU (una sola vez para no recalentar la máquina)
ocr = PaddleOCR(use_angle_cls=True, lang='es', use_gpu=False)

def extract_text_from_pdf(pdf_path: str):
    """
    Convierte un PDF escaneado en texto usando PaddleOCR (CPU).
    Retorna un string con el texto reconocido.
    """
    text_output = []

    # Convertir páginas a imágenes temporales
    with tempfile.TemporaryDirectory() as tmpdir:
        images = convert_from_path(pdf_path, dpi=200, output_folder=tmpdir)
        for i, img in enumerate(images):
            img_path = os.path.join(tmpdir, f"page_{i}.png")
            img.save(img_path, "PNG")

            # OCR por página
            result = ocr.ocr(img_path, cls=True)
            for line in result[0]:
                text_output.append(line[1][0])  # el texto reconocido

    return "\n".join(text_output)


def extract_table_from_pdf(pdf_path: str):
    """
    Intenta reconstruir una tabla básica a partir de OCR.
    Devuelve una lista de filas (listas de celdas).
    """
    tables = []

    with tempfile.TemporaryDirectory() as tmpdir:
        images = convert_from_path(pdf_path, dpi=200, output_folder=tmpdir)
        for i, img in enumerate(images):
            img_path = os.path.join(tmpdir, f"page_{i}.png")
            img.save(img_path, "PNG")

            result = ocr.ocr(img_path, cls=True)

            # Cada línea → tratamos como una fila con palabras separadas por espacios
            for line in result[0]:
                text_line = line[1][0]
                row = text_line.split()
                tables.append(row)

    return tables
