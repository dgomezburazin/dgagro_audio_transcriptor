import os
import io
import re
import json
import datetime
import hashlib
from collections import Counter

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload

from docx import Document
from pydub import AudioSegment
import whisper
from tqdm import tqdm

# ==========================================================
# CONFIGURACIÓN DE CARPETAS EN DRIVE
# ==========================================================

# Nombres de los JSON necesarios
LOG_NAME = ".processed_log.json"
MEMORIA_NAME = "diccionario_campos.json"

# Carpeta donde subís los audios crudos + jsons
FOLDER_AUDIO_ID = "1Wn_4pZm3QXVXCIwG9haPD8IzzTFZhLYn"

# Carpeta raíz donde se guardarán las transcripciones
FOLDER_TRANSCRIPCIONES_ID = "1aDnxpvJSohDfzDq4r5I-fuyxq3BEQ0MD"




# ==========================================================
# AUTENTICACIÓN GOOGLE DRIVE
# ==========================================================
def get_drive_service():
    creds_json = os.environ["GDRIVE_KEY"]
    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)


drive = get_drive_service()


# ==========================================================
# UTILIDADES DRIVE
# ==========================================================
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


def subir_o_actualizar_archivo_contenido(nombre, folder_id, data_bytes, mime_type, existing_id=None):
    bio = io.BytesIO(data_bytes)
    media = MediaIoBaseUpload(bio, mimetype=mime_type, resumable=False)

    if existing_id:
        file_metadata = {"name": nombre}
        drive.files().update(
            fileId=existing_id,
            body=file_metadata,
            media_body=media
        ).execute()
        return existing_id
    else:
        file_metadata = {
            "name": nombre,
            "parents": [folder_id]
        }
        created = drive.files().create(
            body=file_metadata,
            media_body=media,
            fields="id"
        ).execute()
        return created["id"]


def buscar_archivo_por_nombre(nombre, folder_id):
    q = f"'{folder_id}' in parents and name = '{nombre}' and trashed = false"
    resp = drive.files().list(q=q, fields="files(id, name)").execute()
    files = resp.get("files", [])
    if files:
        return files[0]
    return None


def asegurar_subcarpeta_fecha(fecha_str):
    """
    Crea (si no existe) una subcarpeta dentro de FOLDER_TRANSCRIPCIONES_ID
    con nombre = fecha_str (YYYY-MM-DD) y devuelve su ID.
    """
    existente = buscar_archivo_por_nombre(fecha_str, FOLDER_TRANSCRIPCIONES_ID)
    if existente:
        return existente["id"]

    metadata = {
        "name": fecha_str,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [FOLDER_TRANSCRIPCIONES_ID]
    }
    carpeta = drive.files().create(body=metadata, fields="id").execute()
    return carpeta["id"]


# ==========================================================
# UTILIDADES LOCALES
# ==========================================================
def hash_archivo(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def duracion_min(path):
    try:
        a = AudioSegment.from_file(path)
        return round(len(a) / 60000, 1)
    except Exception:
        return None


# ==========================================================
# DETECCIÓN DE NOMBRE DE CAMPO / LOTE (igual que tu Colab)
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
        nombre = Counter(candidatos).most_common(1)[0][0].strip().title()
        memoria[nombre] = memoria.get(nombre, 0) + 1
        return nombre

    capitalizadas = re.findall(r"\b[A-ZÁÉÍÓÚÑ][a-záéíóúñ]{3,}\b", texto)
    if capitalizadas:
        posible = capitalizadas[0].title()
        memoria[posible] = memoria.get(posible, 0) + 1
        return posible

    return "Sin identificar"


# ==========================================================
# CREACIÓN DE DOCX
# ==========================================================
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


def crear_o_actualizar_maestro(fecha_dia, items_fecha, carpeta_fecha_id):
    """
    Crea o actualiza el Word maestro por día dentro de la subcarpeta de fecha.
    """
    nombre_maestro = f"Transcripciones_Completas_{fecha_dia}.docx"
    existente = buscar_archivo_por_nombre(nombre_maestro, carpeta_fecha_id)

    local_path = f"maestro_{fecha_dia}.docx"

    if existente:
        # Descargar existente y agregar contenido
        content = descargar_archivo(existente["id"])
        with open(local_path, "wb") as f:
            f.write(content)
        doc = Document(local_path)
        doc.add_paragraph("")
        doc.add_paragraph("──────────────────────────────")
        doc.add_paragraph(f"Nuevas transcripciones agregadas el {fecha_dia}")
        print(f"📎 Actualizando maestro existente: {nombre_maestro}")
        maestro_id = existente["id"]
    else:
        # Crear nuevo
        doc = Document()
        doc.add_heading("Lomas_Pampeanas – Compilado General de Transcripciones", 0)
        doc.add_paragraph(f"Actualizado al {fecha_dia}")
        doc.add_paragraph("")
        print(f"📄 Creando nuevo maestro: {nombre_maestro}")
        maestro_id = None

    # Agregar ítems
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

    # Subir/actualizar al Drive
    with open(local_path, "rb") as f:
        data = f.read()
    maestro_id = subir_o_actualizar_archivo_contenido(
        nombre_maestro,
        carpeta_fecha_id,
        data,
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        existing_id=maestro_id,
    )
    os.remove(local_path)
    print(f"📘 Maestro diario actualizado: {nombre_maestro} (id={maestro_id})")


# ==========================================================
# CARGA / GUARDADO DE JSONS EN DRIVE
# ==========================================================
def cargar_o_crear_json_en_drive(nombre, folder_id, default_value):
    archivo = buscar_archivo_por_nombre(nombre, folder_id)
    if archivo:
        try:
            data_bytes = descargar_archivo(archivo["id"])
            texto = data_bytes.decode("utf-8").strip()
            if not texto:
                return default_value, archivo["id"]
            return json.loads(texto), archivo["id"]
        except Exception:
            print(f"⚠️ {nombre} dañado, se reinicia.")
            return default_value, archivo["id"]
    else:
        # Crear nuevo vacío
        data_bytes = json.dumps(default_value, ensure_ascii=False, indent=2).encode("utf-8")
        file_id = subir_o_actualizar_archivo_contenido(
            nombre,
            folder_id,
            data_bytes,
            "application/json",
            existing_id=None,
        )
        return default_value, file_id


def guardar_json_en_drive(nombre, folder_id, data, existing_id=None):
    data_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    return subir_o_actualizar_archivo_contenido(
        nombre,
        folder_id,
        data_bytes,
        "application/json",
        existing_id=existing_id,
    )


# ==========================================================
# PROCESAMIENTO PRINCIPAL
# ==========================================================
def main():
    print("🔄 Iniciando transcriptor DG|AGRO360° en GitHub Actions...")

    # 1) Cargar/crear log y memoria desde Drive
    log, log_id = cargar_o_crear_json_en_drive(
        LOG_NAME, FOLDER_AUDIO_ID, {"procesados": {}}
    )
    memoria_campos, memoria_id = cargar_o_crear_json_en_drive(
        MEMORIA_NAME, FOLDER_AUDIO_ID, {}
    )

    # 2) Listar audios en carpeta de origen
    archivos = listar_archivos_carpeta(FOLDER_AUDIO_ID)
    exts = (".mp3", ".m4a", ".wav", ".ogg", ".flac", ".aac")
    audios = [a for a in archivos if a["name"].lower().endswith(exts)]

    print(f"🔍 Audios encontrados en carpeta de origen: {len(audios)}")

    nuevos = []
    for a in audios:
        if a["id"] not in log["procesados"]:
            nuevos.append(a)

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
            fecha_archivo = datetime.date.today().isoformat()

        # Duración
        dur_min = duracion_min(temp_audio)

        # Transcripción
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

        # Crear carpeta por fecha dentro de FOLDER_TRANSCRIPCIONES_ID
        carpeta_fecha_id = asegurar_subcarpeta_fecha(fecha_archivo)

        # Crear docx individual
        campo_slug = campo_detectado.replace(" ", "_")
        nombre_docx = f"{campo_slug}_{fecha_archivo}.docx"
        local_docx = f"doc_{file_id}.docx"
        crear_docx_audio(local_docx, meta, texto)

        with open(local_docx, "rb") as f:
            data_docx = f.read()
        subir_o_actualizar_archivo_contenido(
            nombre_docx,
            carpeta_fecha_id,
            data_docx,
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            existing_id=None,
        )

        # Limpiar temporales
        os.remove(temp_audio)
        os.remove(local_docx)

        # Actualizar log en memoria
        log["procesados"][file_id] = {
            "nombre": nombre,
            "campo": campo_detectado,
            "fecha": fecha_archivo,
        }

    # 4) Crear/actualizar maestros por fecha
    agrupados_por_fecha = {}
    for it in resumen_items:
        agrupados_por_fecha.setdefault(it["fecha_archivo"], []).append(it)

    for fecha_dia, lista_dia in agrupados_por_fecha.items():
        carpeta_fecha_id = asegurar_subcarpeta_fecha(fecha_dia)
        crear_o_actualizar_maestro(fecha_dia, lista_dia, carpeta_fecha_id)

    # 5) Guardar log y memoria de vuelta al Drive
    guardar_json_en_drive(LOG_NAME, FOLDER_AUDIO_ID, log, existing_id=log_id)
    guardar_json_en_drive(MEMORIA_NAME, FOLDER_AUDIO_ID, memoria_campos, existing_id=memoria_id)

    print("✅ Proceso completo: audios nuevos transcritos y subidos a Google Drive.")


if __name__ == "__main__":
    main()



