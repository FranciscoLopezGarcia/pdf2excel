from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
from werkzeug.utils import secure_filename
import jwt, datetime, os, io, zipfile
import pandas as pd
from extractors.universal_extractor import UniversalBankExtractor

# Config
SECRET_KEY = "super_secret_key"  # ⚠️ Cambiar y guardar en config.py en producción
UPLOAD_FOLDER = "input"
OUTPUT_FOLDER = "output"

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Flask app
app = Flask(__name__)
CORS(app)


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
            return jsonify({"error": "Token faltante"}), 401
        try:
            data = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
            request.user = data
        except Exception as e:
            return jsonify({"error": "Token inválido", "detail": str(e)}), 401
        return f(*args, **kwargs)
    return wraps(f)(wrapper)


# --- LOGIN ---
@app.route("/api/login", methods=["POST"])
def login():
    data = request.json
    username = data.get("username")
    password = data.get("password")

    # ⚠️ Simulación: cambiar cuando tengas la BD
    if username == "admin" and password == "admin123":
        role = "admin"
    elif username == "user" and password == "user123":
        role = "user"
    else:
        return jsonify({"error": "Credenciales inválidas"}), 401

    token = jwt.encode({
        "username": username,
        "role": role,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=8)
    }, SECRET_KEY, algorithm="HS256")

    return jsonify({"token": token, "role": role})


# --- CONVERSIÓN ---
@app.route("/api/convert", methods=["POST"])
@token_required
def convert():
    extractor = UniversalBankExtractor()
    uploaded_files = request.files.getlist("files")

    if not uploaded_files:
        return jsonify({"error": "No se enviaron archivos"}), 400

    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as zipf:
        combined = []
        for file in uploaded_files:
            filename = secure_filename(file.filename)
            temp_path = os.path.join(UPLOAD_FOLDER, filename)
            file.save(temp_path)

            try:
                df = extractor.extract_from_pdf(temp_path)
                if df.empty:
                    zipf.writestr(f"{filename}-ERROR.txt", "No se detectaron transacciones")
                    continue

                # Guardar excel individual
                excel_bytes = io.BytesIO()
                df.to_excel(excel_bytes, index=False, sheet_name="Transactions")
                zipf.writestr(f"{os.path.splitext(filename)[0]}.xlsx", excel_bytes.getvalue())

                # Acumular para consolidado
                df["archivo"] = filename
                combined.append(df)

            except Exception as e:
                zipf.writestr(f"{filename}-ERROR.txt", str(e))

        # Consolidado
        if combined:
            df_all = pd.concat(combined, ignore_index=True)
            excel_bytes = io.BytesIO()
            df_all.to_excel(excel_bytes, index=False, sheet_name="Consolidado")
            zipf.writestr("consolidado.xlsx", excel_bytes.getvalue())

    output.seek(0)
    return send_file(output, mimetype="application/zip",
                     as_attachment=True, download_name="resultado.zip")


# --- LOGS (dummy) ---
@app.route("/api/logs", methods=["GET"])
@token_required
def logs():
    # ⚠️ Simulación: más adelante conectar a DB o JSON persistente
    data = [
        {"user": "fran", "date": "2025-09-24", "ok": 3, "errors": 1, "reason": "OCR falló"},
        {"user": "ana", "date": "2025-09-23", "ok": 5, "errors": 0, "reason": ""}
    ]
    return jsonify(data)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
