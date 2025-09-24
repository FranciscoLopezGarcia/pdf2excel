from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import jwt, datetime, os, io, zipfile, logging
import pandas as pd
from extractors.universal_extractor import UniversalBankExtractor

# Config logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
SECRET_KEY = "super_secret_key"  # ⚠️ cambiar en producción y mover a config.py
UPLOAD_FOLDER = "input"
OUTPUT_FOLDER = "output"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Flask app
app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)



# --- Helper: validar JWT ---
def token_required(f):
    from functools import wraps
    def wrapper(*args, **kwargs):
        token = None
        if "Authorization" in request.headers:
            auth_header = request.headers["Authorization"]
            if auth_header.startswith("Bearer "):
                token = auth_header.split(" ")[1]
        if not token:
            logger.warning("Token faltante en request")
            return jsonify({"error": "Token faltante"}), 401
        try:
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            request.user = data
            logger.info(f"Token válido para usuario: {data.get('username')}")
        except Exception as e:
            logger.error(f"Token inválido: {e}")
            return jsonify({"error": "Token inválido", "detail": str(e)}), 401
        return f(*args, **kwargs)
    return wraps(f)(wrapper)


# --- LOGIN ---
@app.route("/api/login", methods=["POST"])
def login():
    logger.info("Login attempt")
    data = request.json
    username = data.get("username")
    password = data.get("password")

    logger.info(f"Login intento para usuario: {username}")

    # ⚠️ Hardcode para pruebas, después conectar a DB
    if username == "admin" and password == "admin123":
        role = "admin"
    elif username == "user" and password == "user123":
        role = "user"
    else:
        logger.warning(f"Credenciales inválidas para: {username}")
        return jsonify({"error": "Credenciales inválidas"}), 401

    token = jwt.encode({
        "username": username,
        "role": role,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    }, SECRET_KEY, algorithm="HS256")

    logger.info(f"Login exitoso para: {username} (role: {role})")
    return jsonify({"token": token, "role": role})


# --- CONVERSIÓN ---
@app.route("/api/convert", methods=["POST"])
@token_required
def convert():
    logger.info("=== INICIO CONVERSIÓN ===")
    logger.info(f"Usuario: {request.user.get('username')}")
    
    extractor = UniversalBankExtractor()
    uploaded_files = request.files.getlist("files")

    logger.info(f"Archivos recibidos: {len(uploaded_files)}")
    for file in uploaded_files:
        logger.info(f"  - {file.filename} ({file.content_length} bytes)")

    if not uploaded_files:
        logger.error("No se enviaron archivos")
        return jsonify({"error": "No se enviaron archivos"}), 400

    output = io.BytesIO()
    zipf = zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED)

    combined = []
    log_messages = []  # para log.txt dentro del ZIP

    for file in uploaded_files:
        filename = secure_filename(file.filename)
        temp_path = os.path.join(UPLOAD_FOLDER, filename)
        
        logger.info(f"Procesando: {filename}")
        logger.info(f"Guardando temporalmente en: {temp_path}")
        
        file.save(temp_path)
        
        # Verificar que el archivo se guardó correctamente
        if not os.path.exists(temp_path):
            logger.error(f"Error: archivo temporal no existe: {temp_path}")
            continue
            
        file_size = os.path.getsize(temp_path)
        logger.info(f"Archivo guardado: {file_size} bytes")

        try:
            logger.info(f"Iniciando extracción para: {filename}")
            df = extractor.extract_from_pdf(temp_path)
            
            if df is None:
                logger.error(f"Extractor devolvió None para: {filename}")
                msg = f"{filename}: ERROR - Extractor devolvió None"
                zipf.writestr(f"{filename}-ERROR.txt", msg)
                log_messages.append(msg)
                continue
                
            if df.empty:
                logger.warning(f"DataFrame vacío para: {filename}")
                msg = f"{filename}: ERROR - No se detectaron transacciones"
                zipf.writestr(f"{filename}-ERROR.txt", msg)
                log_messages.append(msg)
                continue

            logger.info(f"Extracción exitosa: {len(df)} filas para {filename}")
            
            # Guardar excel individual
            excel_bytes = io.BytesIO()
            df.to_excel(excel_bytes, index=False, sheet_name="Transactions")
            excel_data = excel_bytes.getvalue()
            
            logger.info(f"Excel generado: {len(excel_data)} bytes para {filename}")
            zipf.writestr(f"{os.path.splitext(filename)[0]}.xlsx", excel_data)

            # Acumular para consolidado
            df["archivo"] = filename
            combined.append(df)
            log_messages.append(f"{filename}: OK - {len(df)} filas exportadas")

        except Exception as e:
            logger.error(f"Error procesando {filename}: {str(e)}", exc_info=True)
            msg = f"{filename}: ERROR - {str(e)}"
            zipf.writestr(f"{filename}-ERROR.txt", str(e))
            log_messages.append(msg)
        
        finally:
            # Limpiar archivo temporal
            try:
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                    logger.info(f"Archivo temporal eliminado: {temp_path}")
            except Exception as e:
                logger.warning(f"Error eliminando temporal {temp_path}: {e}")

    # Consolidado
    if combined:
        logger.info(f"Generando consolidado con {len(combined)} archivos")
        df_all = pd.concat(combined, ignore_index=True)
        excel_bytes = io.BytesIO()
        df_all.to_excel(excel_bytes, index=False, sheet_name="Consolidado")
        consolidado_data = excel_bytes.getvalue()
        
        logger.info(f"Consolidado generado: {len(consolidado_data)} bytes")
        zipf.writestr("consolidado.xlsx", consolidado_data)
        log_messages.append("Consolidado generado con éxito")
    else:
        logger.warning("No se pudo generar consolidado")
        log_messages.append("No se pudo generar consolidado")

    # Siempre incluir log.txt
    log_content = "\n".join(log_messages)
    zipf.writestr("log.txt", log_content)
    logger.info(f"Log incluido: {len(log_content)} caracteres")

    zipf.close()
    zip_size = len(output.getvalue())
    output.seek(0)
    
    logger.info(f"ZIP final generado: {zip_size} bytes")
    logger.info("=== FIN CONVERSIÓN ===")

    return send_file(
        output,
        mimetype="application/zip",
        as_attachment=True,
        download_name="resultado.zip"
    )


# --- LOGS (dummy) ---
@app.route("/api/logs", methods=["GET"])
@token_required
def logs():
    # ⚠️ Simulación: conectar a DB o JSON más adelante
    data = [
        {"user": "fran", "date": "2025-09-24", "ok": 3, "errors": 1, "reason": "OCR falló"},
        {"user": "ana", "date": "2025-09-23", "ok": 5, "errors": 0, "reason": ""}
    ]
    return jsonify(data)


if __name__ == "__main__":
    logger.info("Iniciando servidor Flask...")
    app.run(port=5000, debug=True)