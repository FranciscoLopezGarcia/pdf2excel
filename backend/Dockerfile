# Imagen base
FROM python:3.10-slim

# Evitar preguntas interactivas
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    poppler-utils \
    ghostscript \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Directorio de trabajo
WORKDIR /app

# Copiar requirements e instalar
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar todo el código
COPY . .

# Crear carpetas input/output (por si no existen)
RUN mkdir -p input output

# Comando por defecto → ejecutar tu script principal
CMD ["python", "-m", "backend.pdf2xls"]
