# PDF2XLS - Bank Statement Extractor

Este proyecto convierte extractos bancarios en **archivos Excel** listos para análisis.

## 🚀 Características
- Lee PDFs de resúmenes bancarios.
- Extrae transacciones con **Camelot** (tablas) o **pdfplumber** (texto).
- Fallback con **OCR (Tesseract + Poppler)** si el PDF es una imagen.
- Exporta resultados a Excel (`.xlsx`).

## 📂 Estructura
backend/
├── extractors/
│ ├── ocr_extractor.py
│ └── universal_extractor.py
├── input/ # PDFs a procesar
├── output/ # Excel generados
├── config.py # Configuración de Poppler/Tesseract
├── pdf2xls.py # Script principal

## 🔧 Requisitos
- Python 3.10+
- Tesseract OCR
- Poppler

### Ubuntu/Debian
```bash
sudo apt-get update && sudo apt-get install -y \
    tesseract-ocr \
    poppler-utils \
    python3-dev \
    build-essential
Windows
Instalar Tesseract OCR

Instalar Poppler

Configurar rutas en config.py:

python
Copiar código
POPPLER_PATH = r"C:\path\to\poppler\bin"
TESSERACT_PATH = r"C:\path\to\tesseract.exe"
▶️ Uso
Copiar los PDFs en la carpeta input/.

Ejecutar:

bash
Copiar código
python -m backend.pdf2xls
Revisar resultados en output/.