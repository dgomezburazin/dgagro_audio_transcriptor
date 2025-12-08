import os
import io
import json
import re
import datetime
import hashlib
import requests
from collections import Counter
from docx import Document
from pydub import AudioSegment
import whisper
from tqdm import tqdm

# ==========================================================
# SUPABASE CONFIG
# ==========================================================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

BASE = f"{SUPABASE_URL}/storage/v1/object"
BUCKET = "dgagro360-transcripciones"  # nombre EXACTO del bucket

def supabase_download(path):
    """Descargar archivo desde Supabase Storage"""
    url = f"{BASE}/{path}"
    headers = {"Authorization": f"Bearer {SUPABASE_KEY}"}
    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        return r.content
    return None  # si no existe

def supabase_upload(path, bytes_data, mime="application/octet-stream"):
    """Subir archivo a Supabase Storage"""
    url = f"{BASE}/{path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": mime,
        "x-upsert": "true"
    }
    r = requests.put(url, headers=headers, data=bytes_data)
    return r.status_code in (200, 201)

def supabase_list(prefix):
    """Listar archivos en un folder"""
    url = f"{BASE}/list/{BUCKET}"
    headers = {"Authorization": f"Bearer {SUPABASE_KEY}"}
    r = requests.post(url, headers=headers, json={"prefix": prefix})
    if r.status_code == 200:
        return r.json()
    return []

# ==========================================================
# JSON LOAD / SAVE
# ==========================================================
def cargar_json_or_default(path, default):
    data = supabase_download(path)
    if not data:
        return default
    try:
        return json.loads(data.decode("utf-8"))
    except:
        return default

def guardar_json(path, data):
    guardar = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    supabase_upload(path, guardar, mime="application/json")

# ==========================================================
# UTILIDADES
# ==========================================================
def detectar_nombre_campo(texto, memoria):
    texto_lower = texto.lower()
    patrones = [
        r"campo\s+(?:de\s+)?([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)",
        r"lote\s+(?:de\s+)?([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+(?:\s+[A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)*)",
    ]

    candidatos = []
    for pat in patrones:
        candidatos += re.findall(pat, texto, flags=re.IGNORECASE)

    for conocido in memoria.keys():
        if conocido.lower() in texto_lower:
            candidatos.append(conocido)

    if candidatos:
        elegido = Counter(candidatos).most_common(1)[0][0].strip().title()
        memoria[elegido] = memoria.get(elegido, 0) + 1
        return elegido

    gen = re.findall(r"\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{3,}\b", texto)
    if gen:
        elegido = gen[0].title()
        memoria[elegido] = memoria.get(elegido, 0) + 1
        return elegido

    return "Sin_identificar"

def crear_docx(meta, texto):
    doc = Document()
    doc.add_heading(f"DG|AGRO360° – Transcripción: {meta['campo_detectado']}", 0)
    doc.add_paragraph(f"📅 Fecha: {meta['fecha_archivo']}")
    doc.add_paragraph(f"📁 Archivo: {meta['nombre']}")
    doc.add_paragraph(f"⏱ Duración: {meta['duracion_min']} min")
    doc.add_heading("Contenido", level=1)
    doc.add_paragraph(texto)

    filename = f"{meta['campo_detectado'].replace(' ','_')}_{meta['fecha_archivo']}.docx"
    doc.save(filename)
    return filename

# ==========================================================
# PROCESO PRINCIPAL
# ==========================================================
def main():
    print("🔄 Iniciando transcriptor DG|AGRO360° con SUPABASE...")

    # JSONS
    log = cargar_json_or_default(f"{BUCKET}/logs/.processed_log.json", {"procesados": {}})
    memoria = cargar_json_or_default(f"{BUCKET}/logs/diccionario_campos.json", {})

    # LISTA AUDIOS NUEVOS
    archivos = supabase_list("audios/")
    audios = [a for a in archivos if a["name"].lower().endswith((".m4a", ".mp3", ".wav"))]

    nuevos = [a for a in audios if a["id"] not in log["procesados"]]

    if not nuevos:
        print("No hay audios nuevos.")
        return

    print(f"🎧 Audios nuevos: {len(nuevos)}")

    modelo = whisper.load_model("small")

    for a in tqdm(nuevos):
        nombre = a["name"]
        file_path = f"{BUCKET}/audios/{nombre}"
        data = supabase_download(file_path)

        if not data:
            continue

        # guardar temp
        temp_file = "temp_audio.m4a"
        with open(temp_file, "wb") as f:
            f.write(data)

        fecha = re.search(r"(\d{4}-\d{2}-\d{2})", nombre)
        fecha_archivo = fecha.group(1) if fecha else str(datetime.date.today())

        try:
            dur_min = round(len(AudioSegment.from_file(temp_file)) / 60000, 1)
        except:
            dur_min = None

        out = modelo.transcribe(temp_file, fp16=False)
        texto = out["text"].strip()

        campo = detectar_nombre_campo(texto, memoria)

        meta = {
            "nombre": nombre,
            "fecha_archivo": fecha_archivo,
            "duracion_min": dur_min,
            "campo_detectado": campo,
        }

        # Crear DOCX
        docx_name = crear_docx(meta, texto)

        with open(docx_name, "rb") as f:
            supabase_upload(
                f"{BUCKET}/transcripciones/{fecha_archivo}/{docx_name}",
                f.read(),
                mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )

        os.remove(docx_name)
        os.remove(temp_file)

        log["procesados"][a["id"]] = meta

    # Guardar JSONS
    guardar_json(f"{BUCKET}/logs/.processed_log.json", log)
    guardar_json(f"{BUCKET}/logs/diccionario_campos.json", memoria)

    print("✅ TODO PROCESADO Y SUBIDO A SUPABASE.")

if __name__ == "__main__":
    main()






