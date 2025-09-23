import os

# Ruta de Poppler (ajustada a tu carpeta actual)
POPPLER_PATH = r"C:\Users\FranciscoLópezGarcía\Downloads\Release-25.07.0-0 (2)\poppler-25.07.0\Library\bin"

# Ruta de Tesseract
TESSERACT_PATH = r"C:\Users\FranciscoLópezGarcía\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"



# Configuración de OCR
TESSERACT_CONFIG = {
    'lang': 'spa',
    'oem': 3,  # Motor OCR más moderno
    'psm': 6,  # Página única uniforme
    'dpi': 350
}