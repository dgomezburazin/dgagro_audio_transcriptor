import os
import io
import json
import datetime
import hashlib
import requests
from collections import Counter
from docx import Document
from pydub import AudioSegment
import whisper
from tqdm import tqdm


# ==========================================================
# 🔧 CONFIG SUPABASE (USAR VARIABLES DE ENTORNO GITHUB)
# ==========================================================
SUPABASE_URL = os.environ["SUPABASE_URL"].strip()
SUPABASE_KEY = os.environ["SUPABASE_KEY"].strip()

# 🟢 ESTE ES TU BUCKET REAL (CORREGIDO)
BUCKET = "dgagro360-transcripciones"

HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}


# ==========================================================
# 📂 UTILIDADES SUPABASE
# ==========================================================
def sb_download(path):
    """Descarga archivo desde Supabase Storage."""
    url = f"{SUPABASE_URL}/storage/v1/object/{path}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code == 200:
        return r.content
    return None


def sb_upload(path, data_bytes, content_type):
    """Sube archivo al Storage."""
    url = f"{SUPABASE_URL}/storage/v1/object/{path}"
    r = requests.put(url, headers={"Content-Type": content_type, **HEADERS}, data=data_bytes)
    if r.status_code not in (200, 201):
        print("❌ ERROR al subir:", r.text)
    return r.text


def load_json(path, default):
    """Carga JSON o lo crea si no existe."""
    data = sb_download(path)
    if data:
        try:
            return json.loads(data.decode("utf-8"))
        except:
            return default
    else:
        sb_upload(path, json.dumps(default).encode("utf-8"), "application/json")
        return default


def save_json(path, data):
    sb_upload(path, json.dumps(data, indent=2).encode("utf-8"), "application/json")


# ==========================================================
# 🎧 UTILIDADES LOCALES
# ==========================================================
def detect_campo(texto, memoria):
    texto_lower = texto.lower()
    patterns = [
        r"campo\s+([A-Za-zÁÉÍÓÚÑáéíóú\s]+)",
        r"lote\s+([A-Za-zÁÉÍÓÚÑáéíóú\s]+)",
    ]

    candidatos = []
    for patt in patterns:
        import re
        encontrados = re.findall(patt, texto, flags=re.IGNORECASE)
        candidatos.extend([x.strip().title() for x in encontrados])

    for known in memoria.keys():
        if known.lower() in texto_lower:
            candidatos.append(known)

    if candidatos:
        elegido = Counter(candidatos).most_common(1)[0][0]
        memoria[elegido] = memoria.get(elegido, 0) + 1
        return elegido

    return "Sin_identificar"


def crear_docx(path, meta, texto):
    doc = Document()
    doc.add_heading(f"Transcripción – {meta['campo']}", 0)
    doc.add_paragraph(f"Fecha: {meta['fecha']}")
    doc.add_paragraph(f"Duración: {meta['duracion']} min")
    doc.add_paragraph(f"Archivo: {meta['nombre']}")
    doc.add_paragraph("")
    doc.add_heading("Texto", level=1)
    doc.add_paragraph(texto)
    doc.save(path)


# ==========================================================
# 🚀 MAIN
# ==========================================================
def main():
    print("🔄 Iniciando transcriptor DG|AGRO360° con SUPABASE...")

    # Carpeta lógica dentro del bucket
    PATH_AUDIOS = f"{BUCKET}/audios"
    PATH_TRANSCRIPCIONES = f"{BUCKET}/transcripciones"
    PATH_LOG = f"{BUCKET}/logs/.processed_log.json"
    PATH_MEM = f"{BUCKET}/logs/diccionario_campos.json"

    # Load JSONs
    log = load_json(PATH_LOG, {"procesados": {}})
    memoria = load_json(PATH_MEM, {})

    # Listar audios (requiere REST API ListObjects)
    list_url = f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET}"
    r = requests.post(list_url, headers=HEADERS, json={"prefix": "audios"})
    audios = r.json()

    nuevos = [a for a in audios if a["id"] not in log["procesados"] and a["name"].lower().endswith((".mp3",".m4a",".wav",".ogg"))]

    if not nuevos:
        print("✅ No hay audios nuevos.")
        return

    print(f"🎧 Audios nuevos: {len(nuevos)}")

    modelo = whisper.load_model("small")

    for audio in tqdm(nuevos):
        nombre = audio["name"]
        file_id = audio["id"]

        # Descargar audio
        contenido = sb_download(f"{BUCKET}/{nombre}") or sb_download(f"{BUCKET}/audios/{nombre}")
        temp = "temp_audio"
        open(temp, "wb").write(contenido)

        # Datos
        fecha = datetime.date.today().isoformat()
        dur = round(len(AudioSegment.from_file(temp)) / 60000, 1)

        # Transcribir
        res = modelo.transcribe(temp, fp16=False)
        texto = res["text"].strip()

        campo = detect_campo(texto, memoria)

        meta = {"nombre": nombre, "fecha": fecha, "duracion": dur, "campo": campo}

        # Crear DOCX local
        docx_name = f"{campo.replace(' ','_')}_{fecha}.docx"
        crear_docx(docx_name, meta, texto)

        # Subir DOCX
        sb_upload(f"{BUCKET}/transcripciones/{docx_name}",
                  open(docx_name,"rb").read(),
                  "application/vnd.openxmlformats-officedocument.wordprocessingml.document")

        # Registrar log
        log["procesados"][file_id] = meta

        os.remove(temp)
        os.remove(docx_name)

    # Guardar JSONs
    save_json(PATH_LOG, log)
    save_json(PATH_MEM, memoria)

    print("✅ COMPLETADO. Transcripciones subidas a SUPABASE.")


if __name__ == "__main__":
    main()







