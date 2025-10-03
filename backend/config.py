import os

# Ruta de Poppler (ajustada a tu carpeta actual)

# Ruta de Tesseract





# config.py
import os
from pathlib import Path

# ===============================================
# CONFIGURACIÓN DE TESSERACT Y POPPLER
# ===============================================

# Detección automática del sistema operativo
import platform
sistema = platform.system().lower()

if sistema == "windows":
    # WINDOWS - Ajusta estas rutas según tu instalación
    TESSERACT_PATH = r"C:\Users\FranciscoLópezGarcía\AppData\Local\Programs\Tesseract-OCR\tesseract.exe"
    POPPLER_PATH = r"C:\Users\FranciscoLópezGarcía\Downloads\Release-25.07.0-0 (2)\poppler-25.07.0\Library\bin"

    # Configuración de OCR
    TESSERACT_CONFIG = {
    'lang': 'spa',
    'oem': 3,  # Motor OCR más moderno
    'psm': 6,  # Página única uniforme
    'dpi': 350
}

    # Alternativas comunes en Windows:
    # TESSERACT_PATH = r"C:\Users\{usuario}\AppData\Local\Tesseract-OCR\tesseract.exe"
    # POPPLER_PATH = r"C:\poppler\bin"
    
elif sistema == "darwin":  # macOS
    # macOS - Instalación típica con Homebrew
    TESSERACT_PATH = "/usr/local/bin/tesseract"  # o "/opt/homebrew/bin/tesseract"
    POPPLER_PATH = "/usr/local/bin"  # o "/opt/homebrew/bin"
    
else:  # Linux
    # LINUX - Instalación típica con apt/yum
    TESSERACT_PATH = "/usr/bin/tesseract"
    POPPLER_PATH = "/usr/bin"  # Generalmente no necesario en Linux

# ===============================================
# VERIFICACIÓN DE INSTALACIÓN
# ===============================================

def verificar_instalacion():
    """Verifica que Tesseract y Poppler estén correctamente instalados"""
    errors = []
    
    # Verificar Tesseract
    if not os.path.exists(TESSERACT_PATH):
        errors.append(f"❌ Tesseract no encontrado en: {TESSERACT_PATH}")
    else:
        print(f"✅ Tesseract encontrado: {TESSERACT_PATH}")
    
    # Verificar Poppler (solo en Windows)
    if sistema == "windows":
        poppler_exe = os.path.join(POPPLER_PATH, "pdftoppm.exe")
        if not os.path.exists(poppler_exe):
            errors.append(f"❌ Poppler no encontrado en: {POPPLER_PATH}")
            errors.append(f"   Buscando: {poppler_exe}")
        else:
            print(f"✅ Poppler encontrado: {POPPLER_PATH}")
    
    if errors:
        print("\n" + "="*50)
        print("⚠️  ERRORES DE CONFIGURACIÓN:")
        for error in errors:
            print(error)
        print("="*50)
        print("\n📋 INSTRUCCIONES DE INSTALACIÓN:")
        print_installation_guide()
        return False
    
    print("✅ Configuración OK - Todos los componentes encontrados")
    return True

def print_installation_guide():
    """Imprime guía de instalación según el sistema operativo"""
    if sistema == "windows":
        print("""
🪟 WINDOWS:

1. TESSERACT:
   - Descargar: https://github.com/UB-Mannheim/tesseract/wiki
   - Instalar en: C:\\Program Files\\Tesseract-OCR\\
   - Agregar al PATH: C:\\Program Files\\Tesseract-OCR\\

2. POPPLER:
   - Descargar: https://github.com/oschwartz10612/poppler-windows/releases/
   - Extraer en: C:\\Program Files\\poppler-0.68.0\\
   - La carpeta debe contener: bin\\pdftoppm.exe

3. Ajustar rutas en config.py si es necesario
        """)
    
    elif sistema == "darwin":  # macOS
        print("""
🍎 macOS:

1. Instalar Homebrew (si no tienes):
   /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

2. Instalar componentes:
   brew install tesseract
   brew install poppler

3. Las rutas deberían estar correctas automáticamente
        """)
    
    else:  # Linux
        print("""
🐧 LINUX:

Ubuntu/Debian:
   sudo apt update
   sudo apt install tesseract-ocr tesseract-ocr-spa
   sudo apt install poppler-utils

CentOS/RHEL:
   sudo yum install tesseract
   sudo yum install poppler-utils
        """)

# Ejecutar verificación al importar (solo en modo desarrollo)
if __name__ == "__main__":
    print("🔍 Verificando configuración OCR...")
    verificar_instalacion()