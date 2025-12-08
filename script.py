import os
import io
import re
import json
import hashlib
import datetime
from collections import Counter

from dotenv import load_dotenv
import requests
from docx import Document
from pydub import AudioSegment
import whisper
from tqdm import tqdm

# ==========================================================
# 🔐 CARGAR VARIABLES DESDE SECRETS
# ==========================================================
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_ANON_KEY = os.environ["SUPABASE_ANON_KEY"]
SUPABASE_SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE"]

BUCKET = "dgagro360-transcripciones"

# ==========================================================
# 🔧 UTILIDADES SUPABASE
# ==========================================================
def supabase_upload(path, data_bytes, content_type):
    """Sube un archivo al bucket en Supabase Storage."""
    url = f"{SUPABASE_URL}/storage/v1/object/{path}"
    headers = {
        "Content-Type": content_type,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "apikey": SUPABASE_SERVICE_ROLE,
    }
    r = requests.put(url, headers=headers, data=data_bytes)
    if r.status_code not in (200, 201):
        print("❌ Error al subir a Supabase:", r.text)
    return r.status_code


def supabase_download(path):
    """Descarga un archivo desde Supabase Storage."""
    url = f"{SUPABASE_URL}/storage/v1/object/{path}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
        "apikey": SUPABASE_SERVICE_ROLE,
    }
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        return r.content
    return None


def supabase_list(prefix):
    """Lista archivos con un prefijo."""
    url = f"{SUPABASE_URL}/storage/v1/object/list/{BUCKET}"
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE}",
            "apikey": SUPABASE_SERVICE_ROLE,
            "Content-Type": "application/json",
        },
        json={"prefix": prefix, "limit": 2000},
    )
    return r.json()


# ==========================================================
# HASH Y UTILIDADES DE AUDIO
# ==========================================================
def duracion_min(path):
    try:
        a = AudioSegment.from_file(path)
        return round(len(a) / 60000, 1)
    except:
        return None


def detectar_nombre_campo(texto, memoria):
    texto_lower = texto.lower()
    patrones = [
        r"campo\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)",
        r"lote\s+([A-ZÁÉÍÓÚÑ][a-záéíóúñ]+)",
    ]
    candidatos = []

    for pat in patrones:
        candidatos += re.findall(pat, texto, flags=re.IGNORECASE)

    for conocido in memoria.keys():
        if conocido.lower() in texto_lower:
            candidatos.append(conocido)

    if candidatos:
        nombre = Counter(candidatos).most_common(1)[0][0].title()
        memoria[nombre] = memoria.get(nombre, 0) + 1
        return nombre

    capitalizadas = re.findall(r"\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{3,}\b", texto)
    if capitalizadas:
        posible = capitalizadas[0].title()
        memoria[posible] = memoria.get(posible, 0) + 1
        return posible

    return "Sin identificar"


# ==========================================================
# JSONS (LOG + MEMORIA)
# ==========================================================
def cargar_json_or_default(path, default):
    content = supabase_download(path)
    if content:
        try:
            return json.loads(content.decode("utf-8"))
        except:
            return default
    else:
        supabase_upload(path, json.dumps(default).encode("utf-8"), "application/json")
        return default


def guardar_json(path, data):
    supabase_upload(path, json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8"),
                    "application/json")


# ==========================================================
# CREAR DOCX
# ==========================================================
def crear_docx(path_local, meta, texto):
    doc = Document()
    doc.add_heading(f"DGAGRO360° – Transcripción: {meta['campo']}", 0)
    doc.add_paragraph(f"📅 Fecha: {meta['fecha']}")
    doc.add_paragraph(f"⏱ Duración: {meta['duracion']} min")
    doc.add_paragraph(f"🎧 Archivo: {meta['nombre']}")
    doc.add_paragraph("")
    doc.add_heading("📝 Transcripción completa", level=1)
    doc.add_paragraph(texto)
    doc.save(path_local)


# ==========================================================
# PROCESAMIENTO PRINCIPAL
# ==========================================================
def main():

    print("🔄 Iniciando transcriptor DG|AGRO360° con SUPABASE...")

    # 1) Cargar JSONs
    log = cargar_json_or_default(f"{BUCKET}/logs/.processed_log.json",
                                 {"procesados": {}})
    memoria = cargar_json_or_default(f"{BUCKET}/logs/diccionario_campos.json", {})

    # 2) Listar audios nuevos
    listado = supabase_list("audios/")
    archivos = [a for a in listado if a["name"].lower().endswith((".m4a", ".mp3", ".wav"))]

    nuevos = [a for a in archivos if a["name"] not in log["procesados"]]

    print(f"🎧 Audios nuevos detectados: {len(nuevos)}")

    if not nuevos:
        print("✔ No hay audios nuevos. Fin.")
        return

    print("🧠 Cargando modelo Whisper...")
    modelo = whisper.load_model("small")

    resumen_por_fecha = {}

    # 3) PROCESAR
    for audio in tqdm(nuevos):

        name = audio["name"]
        full_path = f"{BUCKET}/audios/{name}"

        contenido = supabase_download(full_path)
        if not contenido:
            print("❌ Error al descargar audio:", name)
            continue

        local_temp = f"temp_{name}"
        with open(local_temp, "wb") as f:
            f.write(contenido)

        fecha = re.search(r"\d{4}-\d{2}-\d{2}", name)
        fecha = fecha.group(0) if fecha else datetime.date.today().isoformat()

        dur = duracion_min(local_temp)

        out = modelo.transcribe(local_temp, fp16=False)
        texto = out.get("text", "").strip()

        campo = detectar_nombre_campo(texto, memoria)

        meta = {
            "nombre": name,
            "fecha": fecha,
            "campo": campo,
            "duracion": dur,
        }

        # Guardar resumen
        resumen_por_fecha.setdefault(fecha, []).append({"meta": meta, "texto": texto})

        log["procesados"][name] = {"fecha": fecha, "campo": campo}

        os.remove(local_temp)

    # 4) SUBIR RESULTADOS
    for fecha, items in resumen_por_fecha.items():
        carpeta_fecha = f"{BUCKET}/transcripciones/{fecha}/"

        # Individuales
        for item in items:
            meta = item["meta"]
            texto = item["texto"]
            campo_slug = meta["campo"].replace(" ", "_")
            doc_name = f"{campo_slug}_{fecha}.docx"

            local_doc = f"out_{doc_name}"
            crear_docx(local_doc, meta, texto)

            with open(local_doc, "rb") as f:
                supabase_upload(
                    f"{carpeta_fecha}{doc_name}",
                    f.read(),
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                )

            os.remove(local_doc)

        # Maestro
        maestro_local = f"maestro_{fecha}.docx"
        doc = Document()
        doc.add_heading(f"DGAGRO360° – Maestro diario {fecha}", 0)

        for item in items:
            doc.add_heading(f"📍 {item['meta']['campo']}", level=1)
            doc.add_paragraph(item["texto"])
            doc.add_paragraph("")

        doc.save(maestro_local)

        with open(maestro_local, "rb") as f:
            supabase_upload(
                f"{carpeta_fecha}/maestro_{fecha}.docx",
                f.read(),
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            )

        os.remove(maestro_local)

    # 5) Guardar JSONs actualizados
    guardar_json(f"{BUCKET}/logs/.processed_log.json", log)
    guardar_json(f"{BUCKET}/logs/diccionario_campos.json", memoria)

    print("✔ PROCESO COMPLETADO – Archivos subidos a Supabase.")


if __name__ == "__main__":
    main()




