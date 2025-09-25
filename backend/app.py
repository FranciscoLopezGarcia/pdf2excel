from flask import Flask, request, jsonify, send_file, Response
from flask_cors import CORS
from werkzeug.utils import secure_filename
import jwt, datetime, os, io, zipfile, logging, time
import pandas as pd
from extractors.universal_extractor import UniversalBankExtractor
from extractors.unificador import unir_consolidados


# Config logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Config
SECRET_KEY = "super_secret_key"  # ⚠️ cambiar en producción
UPLOAD_FOLDER = "input"
OUTPUT_FOLDER = "output"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Estado de progreso por usuario
progress_state = {}

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

    # ⚠️ Hardcode para pruebas
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


# --- PROGRESO SSE ---
@app.route("/api/progress")
@token_required
def progress():
    user = request.user["username"]

    def generate():
        last_sent = ""
        while True:
            time.sleep(1)
            state = progress_state.get(user)
            if state and state != last_sent:
                yield f"data: {state}\n\n"
                last_sent = state
            if state and '"progress": 100' in state:
                break

    return Response(generate(), mimetype="text/event-stream")


# --- CONVERSIÓN ---
@app.route("/api/convert", methods=["POST"])
@token_required
def convert():
    logger.info("=== INICIO CONVERSIÓN ===")
    user = request.user.get("username")
    extractor = UniversalBankExtractor()
    uploaded_files = request.files.getlist("files")

    if not uploaded_files:
        return jsonify({"error": "No se enviaron archivos"}), 400

    total_files = len(uploaded_files)
    progress_state[user] = '{"progress": 0, "status": "Iniciando conversión"}'

    output = io.BytesIO()
    zipf = zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED)

    combined = []
    log_messages = []

    for idx, file in enumerate(uploaded_files, start=1):
        filename = secure_filename(file.filename)
        temp_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(temp_path)

        progress_state[user] = (
            f'{{"progress": {int(((idx-1)/total_files)*100)}, '
            f'"status": "Procesando {filename}"}}'
        )

        try:
            df = extractor.extract_from_pdf(temp_path)

            if df is None or df.empty:
                msg = f"{filename}: ERROR - Sin transacciones"
                zipf.writestr(f"{filename}-ERROR.txt", msg)
                log_messages.append(msg)
            else:
                excel_bytes = io.BytesIO()
                df.to_excel(excel_bytes, index=False, sheet_name="Transactions")
                zipf.writestr(f"{os.path.splitext(filename)[0]}.xlsx", excel_bytes.getvalue())
                df["archivo"] = filename
                combined.append(df)
                log_messages.append(f"{filename}: OK - {len(df)} filas exportadas")

        except Exception as e:
            msg = f"{filename}: ERROR - {str(e)}"
            zipf.writestr(f"{filename}-ERROR.txt", msg)
            log_messages.append(msg)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

        progress_state[user] = (
            f'{{"progress": {int((idx/total_files)*100)}, '
            f'"status": "Completado {filename}"}}'
        )

    # Consolidado
    if combined:
        df_all = pd.concat(combined, ignore_index=True)
        excel_bytes = io.BytesIO()
        df_all.to_excel(excel_bytes, index=False, sheet_name="Consolidado")
        zipf.writestr("consolidado.xlsx", excel_bytes.getvalue())
        log_messages.append("Consolidado generado con éxito")
    else:
        log_messages.append("No se pudo generar consolidado")

    zipf.writestr("log.txt", "\n".join(log_messages))
    zipf.close()
    output.seek(0)

    progress_state[user] = '{"progress": 100, "status": "Finalizado"}'

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
    data = [
        {"user": "fran", "date": "2025-09-24", "ok": 3, "errors": 1, "reason": "OCR falló"},
        {"user": "ana", "date": "2025-09-23", "ok": 5, "errors": 0, "reason": ""}
    ]
    return jsonify(data)


# --- UNIFICAR CONSOLIDADOS ---
@app.route("/api/unificar", methods=["POST"])
@token_required
def unificar():
    logger.info("=== INICIO UNIFICADOR ===")
    user = request.user.get("username")
    uploaded_files = request.files.getlist("files")

    if not uploaded_files:
        return jsonify({"error": "No se enviaron archivos"}), 400

    temp_files = []
    for file in uploaded_files:
        filename = secure_filename(file.filename)
        temp_path = os.path.join(UPLOAD_FOLDER, filename)
        file.save(temp_path)
        temp_files.append(temp_path)

    try:
        output_path = os.path.join(OUTPUT_FOLDER, "consolidado_anual.xlsx")
        unir_consolidados(temp_files, output_path)

        return send_file(
            output_path,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="consolidado_anual.xlsx"
        )
    finally:
        # limpieza
        for path in temp_files:
            if os.path.exists(path):
                os.remove(path)



if __name__ == "__main__":
    logger.info("Iniciando servidor Flask...")
    app.run(port=5000, debug=True, threaded=True)
