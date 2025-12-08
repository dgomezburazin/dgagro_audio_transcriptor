import os
import io
import re
import json
import datetime as dt
import hashlib
from collections import Counter

import requests
from tqdm import tqdm
from docx import Document
from pydub import AudioSegment
import whisper

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ==========================================================
# 🔧 CONFIGURACIÓN GOOGLE DRIVE (SOLO LECTURA AUDIOS)
# ==========================================================
# Carpeta compartida donde subís los audios (IDs que me pasaste)
FOLDER_AUDIO_ID = "1Wn_4pZm3QXVXCIwG9haPD8IzzTFZhLYn"

def get_drive_service():
    creds_json = os.environ["GDRIVE_KEY"]
    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)

drive = get_drive_service()

def listar_archivos_carpeta(folder_id, extra_q=None):
    q = f"'{folder_id}' in parents and trashed = false"
    if extra_q:
        q = f"{q} and {extra_q}"

    files = []
    page_token = None
    while True:
        resp = drive.files().list(
            q=q,
            fields="nextPageToken, files(id, name, mimeType)",
            pageToken=page_token
        ).execute()
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files

def descargar_archivo(file_id) -> bytes:
    request = drive.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh.read()

def descargar_archivo_a_fisico(file_id, local_path):
    content = descargar_archivo(file_id)
    with open(local_path, "wb") as f:
        f.write(content)

# ==========================================================
# 🗄️ CONFIGURACIÓN SUPABASE (ALMACENAMIENTO EXTERNO)
# ==========================================================
# ⚠️ Estos deben venir de GitHub Secrets:
#   SUPABASE_URL = "https://wfqawfithisvwlqordjt.supabase.co"
#   SUPABASE_KEY = anon/service key
SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")   # ej: https://wfqawfithisvwlqordjt.supabase.co
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BUCKET = "dgagro360-transcripciones"

BASE_URL = f"{SUPABASE_URL}/storage/v1/object"
BASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
}

def supabase_download(path: str):
    """
    Descarga un archivo del bucket Supabase.
    path es la RUTA DENTRO DEL BUCKET, e.g.:
        logs/.processed_log.json
        docx/maestro/2025-12-08/Transcripciones_Completas_2025-12-08.docx
    """
    url = f"{BASE_URL}/{BUCKET}/{path.lstrip('/')}"
    r = requests.get(url, headers=BASE_HEADERS)
    if r.status_code == 200:
        return r.content
    if r.status_code != 404:
        print(f"⚠️ Supabase download error ({path}): {r.status_code} -> {r.text}")
    return None

def supabase_upload(path: str, bytes_data: bytes, mime: str = "application/octet-stream"):
    """
    Sube (o reemplaza) un archivo en el bucket Supabase.
    """
    url = f"{BASE_URL}/{BUCKET}/{path.lstrip('/')}"
    headers = {
        **BASE_HEADERS,
        "Content-Type": mime,
    }
    r = requests.put(url, headers=headers, data=bytes_data)
    if r.status_code not in (200, 201):
        print(f"❌ Error subiendo a Supabase ({path}): {r.status_code} -> {r.text}")
    else:
        print(f"✅ Subido/actualizado en Supabase: {path}")
    return r

# ==========================================================
# 📓 LOGS Y MEMORIA EN SUPABASE
# ==========================================================
LOG_PATH = "logs/.processed_log.json"
MEMORIA_PATH = "logs/memoria_campos.json"

def cargar_json_or_default(path: str, default_value):
    content = supabase_download(path)
    if content is None:
        print(f"ℹ️ No existe aún {path} en Supabase, se crea desde cero.")
        return default_value
    try:
        txt = content.decode("utf-8").strip()
        if not txt:
            return default_value
        return json.loads(txt)
    except Exception as e:
        print(f"⚠️ Error leyendo JSON {path}, se reinicia. Detalle: {e}")
        return default_value

def guardar_json(path: str, data):
    data_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    supabase_upload(path, data_bytes, mime="application/json")

# ==========================================================
# 🧮 UTILIDADES LOCALES
# ==========================================================
def duracion_min(path):
    try:
        a = AudioSegment.from_file(path)
        return round(len(a) / 60000, 1)
    except Exception:
        return None

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
        nombre = Counter(candidatos).most_common(1)[0][0].strip().title()
        memoria[nombre] = memoria.get(nombre, 0) + 1
        return nombre

    capitalizadas = re.findall(r"\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{3,}\b", texto)
    if capitalizadas:
        posible = capitalizadas[0].title()
        memoria[posible] = memoria.get(posible, 0) + 1
        return posible

    return "Sin identificar"

def crear_docx_audio(salida_path, meta, texto):
    doc = Document()
    doc.add_heading(f"Lomas_Pampeanas – Transcripción: {meta['campo_detectado']}", 0)
    doc.add_paragraph(f"📅 Fecha: {meta['fecha_archivo']}")
    doc.add_paragraph(f"⏱ Duración: {meta['duracion_min']} min")
    doc.add_paragraph(f"📁 Archivo original: {meta['nombre']}")
    doc.add_paragraph("")
    doc.add_heading("📝 Transcripción completa", level=1)
    doc.add_paragraph(texto)
    doc.save(salida_path)

def crear_o_actualizar_maestro(fecha_dia, items_fecha):
    """
    Crea o actualiza el Word maestro por día dentro de Supabase:
      docx/maestro/{fecha}/Transcripciones_Completas_{fecha}.docx
    """
    nombre_maestro = f"Transcripciones_Completas_{fecha_dia}.docx"
    supa_path = f"docx/maestro/{fecha_dia}/{nombre_maestro}"

    local_path = f"maestro_{fecha_dia}.docx"

    # Intentar descargar maestro existente
    contenido = supabase_download(supa_path)
    if contenido:
        with open(local_path, "wb") as f:
            f.write(contenido)
        doc = Document(local_path)
        doc.add_paragraph("")
        doc.add_paragraph("──────────────────────────────")
        doc.add_paragraph(f"Nuevas transcripciones agregadas el {fecha_dia}")
        print(f"📎 Actualizando maestro existente: {nombre_maestro}")
    else:
        doc = Document()
        doc.add_heading("Lomas_Pampeanas – Compilado General de Transcripciones", 0)
        doc.add_paragraph(f"Actualizado al {fecha_dia}")
        doc.add_paragraph("")
        print(f"📄 Creando nuevo maestro: {nombre_maestro}")

    # Agregar por campo
    agrupados = {}
    for it in items_fecha:
        agrupados.setdefault(it["campo_detectado"], []).append(it)

    for campo, lista in agrupados.items():
        doc.add_heading(f"📍 {campo}", level=1)
        for it in sorted(lista, key=lambda x: x["fecha_archivo"]):
            doc.add_heading(f"🎧 Audio – {it['fecha_archivo']}", level=2)
            doc.add_paragraph(it["texto"])
            doc.add_paragraph("")

    doc.save(local_path)

    with open(local_path, "rb") as f:
        data = f.read()
    supabase_upload(
        supa_path,
        data,
        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )
    os.remove(local_path)
    print(f"📘 Maestro diario actualizado: {nombre_maestro}")

# ==========================================================
# 🚀 PROCESAMIENTO PRINCIPAL
# ==========================================================
def main():
    print("🔄 Iniciando transcriptor DG|AGRO360° con SUPABASE...")

    # 1) Cargar log y memoria desde Supabase
    log = cargar_json_or_default(LOG_PATH, {"procesados": {}})
    memoria_campos = cargar_json_or_default(MEMORIA_PATH, {})

    # 2) Listar audios en la carpeta de origen (Drive)
    archivos = listar_archivos_carpeta(FOLDER_AUDIO_ID)
    exts = (".mp3", ".m4a", ".wav", ".ogg", ".flac", ".aac")
    audios = [a for a in archivos if a["name"].lower().endswith(exts)]

    print(f"🔍 Audios encontrados en carpeta de origen (Drive): {len(audios)}")

    procesados_ids = set(log.get("procesados", {}).keys())
    nuevos = [a for a in audios if a["id"] not in procesados_ids]

    if not nuevos:
        print("✅ No hay audios nuevos para procesar. Fin.")
        return

    print(f"🎧 Audios NUEVOS a procesar: {len(nuevos)}")

    # 3) Cargar modelo Whisper una sola vez
    print("🧠 Cargando modelo Whisper 'small'...")
    modelo = whisper.load_model("small")

    resumen_items = []

    for audio in tqdm(nuevos, desc="Transcribiendo"):
        file_id = audio["id"]
        nombre = audio["name"]

        # Descargar audio a archivo temporal
        temp_audio = f"temp_{file_id}.audio"
        descargar_archivo_a_fisico(file_id, temp_audio)

        # Fecha desde nombre o actual
        fecha_match = re.search(r"(\d{4}-\d{2}-\d{2})", nombre)
        if fecha_match:
            fecha_archivo = fecha_match.group(1)
        else:
            fecha_archivo = dt.date.today().isoformat()

        # Duración aprox
        dur_min = duracion_min(temp_audio)

        # Transcripción con Whisper
        out = modelo.transcribe(temp_audio, fp16=False)
        texto = out.get("text", "").strip()

        # Detección de campo
        campo_detectado = detectar_nombre_campo(texto, memoria_campos)

        meta = {
            "id": file_id,
            "nombre": nombre,
            "fecha_archivo": fecha_archivo,
            "duracion_min": dur_min,
            "campo_detectado": campo_detectado,
            "texto": texto,
        }
        resumen_items.append(meta)

        # Crear docx individual y subirlo a Supabase
        campo_slug = re.sub(r"[^A-Za-z0-9_ÁÉÍÓÚÑáéíóúñ]", "_", campo_detectado)
        nombre_docx = f"{campo_slug}_{fecha_archivo}.docx"
        local_docx = f"doc_{file_id}.docx"
        crear_docx_audio(local_docx, meta, texto)

        with open(local_docx, "rb") as f:
            data_docx = f.read()

        supa_path_docx = f"docx/por_audio/{fecha_archivo}/{nombre_docx}"
        supabase_upload(
            supa_path_docx,
            data_docx,
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        )

        # Limpiar temporales
        try:
            os.remove(temp_audio)
        except FileNotFoundError:
            pass
        try:
            os.remove(local_docx)
        except FileNotFoundError:
            pass

        # Actualizar log en memoria
        log["procesados"][file_id] = {
            "nombre": nombre,
            "campo": campo_detectado,
            "fecha": fecha_archivo,
        }

    # 4) Crear/actualizar maestros por fecha en Supabase
    agrupados_por_fecha = {}
    for it in resumen_items:
        agrupados_por_fecha.setdefault(it["fecha_archivo"], []).append(it)

    for fecha_dia, lista_dia in agrupados_por_fecha.items():
        crear_o_actualizar_maestro(fecha_dia, lista_dia)

    # 5) Guardar log y memoria de vuelta en Supabase
    guardar_json(LOG_PATH, log)
    guardar_json(MEMORIA_PATH, memoria_campos)

    print("✅ Proceso completo: audios nuevos transcritos y subidos a Supabase.")


if __name__ == "__main__":
    main()





