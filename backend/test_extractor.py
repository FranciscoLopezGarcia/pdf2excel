#!/usr/bin/env python3
"""
Test script para verificar que el extractor funciona correctamente
"""
import os
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# Agregar el directorio actual al path para imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from extractors.universal_extractor import UniversalBankExtractor
    from config import verificar_instalacion
except ImportError as e:
    print(f"❌ Error de import: {e}")
    print("Asegúrate de que todos los archivos estén en su lugar")
    sys.exit(1)

def test_extractor():
    print("🔧 Verificando configuración...")
    
    # Verificar instalación de dependencias
    if not verificar_instalacion():
        print("❌ Configuración incorrecta. Ver errores arriba.")
        return False
    
    print("\n📁 Buscando archivos PDF de prueba...")
    
    # Buscar PDFs en input/
    input_dir = Path("input")
    if not input_dir.exists():
        input_dir.mkdir()
        print(f"✅ Creada carpeta: {input_dir}")
    
    pdf_files = list(input_dir.glob("*.pdf"))
    if not pdf_files:
        print("⚠️  No hay archivos PDF en la carpeta 'input/'")
        print("   Por favor, coloca al menos un PDF de prueba ahí")
        return False
    
    print(f"📄 Encontrados {len(pdf_files)} archivo(s) PDF:")
    for pdf in pdf_files:
        print(f"   - {pdf.name}")
    
    print("\n🔄 Iniciando extracción de prueba...")
    
    # Probar extractor
    extractor = UniversalBankExtractor()
    
    for pdf_file in pdf_files[:2]:  # Solo los primeros 2 para no saturar
        print(f"\n--- Procesando: {pdf_file.name} ---")
        
        try:
            # Test extracción
            df = extractor.extract_from_pdf(str(pdf_file))
            
            if df is None:
                print(f"❌ {pdf_file.name}: Extractor devolvió None")
                continue
                
            if df.empty:
                print(f"⚠️  {pdf_file.name}: No se encontraron transacciones")
                print("   Esto puede ser normal si el PDF no tiene tablas de movimientos")
                continue
            
            print(f"✅ {pdf_file.name}: {len(df)} transacciones extraídas")
            
            # Mostrar muestra de datos
            print("\n📊 Muestra de datos extraídos:")
            print(df.head().to_string())
            
            # Guardar Excel de prueba
            output_file = f"test_output_{pdf_file.stem}.xlsx"
            df.to_excel(output_file, index=False)
            print(f"💾 Guardado en: {output_file}")
            
        except Exception as e:
            print(f"❌ Error procesando {pdf_file.name}: {e}")
            import traceback
            print("🔍 Detalles del error:")
            traceback.print_exc()
    
    print("\n✅ Test completado")
    return True

if __name__ == "__main__":
    print("🧪 TEST DEL EXTRACTOR PDF2EXCEL")
    print("=" * 40)
    test_extractor()
    print("\n👉 Si hay errores, revisa la configuración en config.py")