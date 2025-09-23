# 📄 PDF2Excel Extractor

Sistema para convertir **extractos y balances bancarios en PDF** a **Excel**.  
Funciona con tablas embebidas en PDFs o, en caso de PDFs escaneados, aplica **OCR (PaddleOCR)**.

---

## 🚀 Tecnologías
- Python 3.11
- PaddleOCR + PaddlePaddle
- Camelot + pdfplumber
- pdf2image (para OCR)
- Docker + docker-compose

---

## 📦 Instalación y ejecución con Docker

### 1. Construir la imagen
```bash
docker compose build --no-cache
