import pandas as pd
import os

def unir_consolidados(excel_files, output_file="consolidado_anual.xlsx"):
    """
    Une múltiples consolidados mensuales (Excel) en un solo consolidado anual.
    - excel_files: lista de rutas a archivos .xlsx
    - output_file: nombre del archivo Excel de salida
    """
    all_data = []
    
    for file in excel_files:
        try:
            df = pd.read_excel(file)

            # Asegurar columnas mes/año
            if "mes" not in df.columns or "año" not in df.columns:
                df["mes"] = pd.to_datetime(df["fecha"], errors="coerce").dt.month
                df["año"] = pd.to_datetime(df["fecha"], errors="coerce").dt.year

            # Agregar columna con el nombre de archivo (traza de origen)
            df["origen"] = os.path.basename(file)
            all_data.append(df)

            print(f"✅ Cargado: {file} ({len(df)} filas)")
        except Exception as e:
            print(f"❌ Error leyendo {file}: {e}")

    if not all_data:
        print("No se cargó ningún consolidado válido")
        return None

    df_all = pd.concat(all_data, ignore_index=True)

    # Ordenar por año, mes y fecha
    if {"año", "mes", "fecha"}.issubset(df_all.columns):
        df_all = df_all.sort_values(by=["año", "mes", "fecha"]).reset_index(drop=True)

    # Exportar
    df_all.to_excel(output_file, index=False, sheet_name="Consolidado_Anual")
    print(f"📊 Consolidado anual generado: {output_file} ({len(df_all)} filas)")
    return df_all
