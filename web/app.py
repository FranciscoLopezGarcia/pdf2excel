import os
import shutil
import tempfile
import zipfile
from flask import Flask, render_template, request, send_file
from extractors.pdf2xls import main as run_extractor

app = Flask(__name__)

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/upload", methods=["POST"])
def upload():
    # Crear carpeta temporal para esta sesión
    tmpdir = tempfile.mkdtemp()

    uploaded_files = request.files.getlist("files[]")
    pdf_paths = []

    for f in uploaded_files:
        pdf_path = os.path.join(tmpdir, f.filename)
        f.save(pdf_path)
        pdf_paths.append(pdf_path)

    # Ejecutar extractor sobre esos PDFs
    output_dir = os.path.join(tmpdir, "output")
    os.makedirs(output_dir, exist_ok=True)

    run_extractor(input_dir=tmpdir, output_dir=output_dir)

    # Empaquetar resultados en un ZIP
    zip_path = os.path.join(tmpdir, "results.zip")
    with zipfile.ZipFile(zip_path, "w") as zipf:
        for root, _, files in os.walk(output_dir):
            for file in files:
                zipf.write(os.path.join(root, file), file)

    # Devolver el ZIP y luego borrar todo
    response = send_file(zip_path, as_attachment=True)
    shutil.rmtree(tmpdir, ignore_errors=True)

    return response

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
