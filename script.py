import os
import io
import json
import datetime
import hashlib
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from docx import Document
from pydub import AudioSegment
import whisper
from tqdm import tqdm

# ========== CONFIGURACIÓN ==========
FOLDER_ID = "1NUq38acTjxIuEvhPgUvFnFlGEOdw8X5i"   # Carpeta DGAGRO_escucha
LOG_FILE = "procesados_log.json"

# Cargar credenciales del secreto
creds_json = os.environ["GDRIVE_KEY"]
creds = service_account.Credentials.from_service_account_info(
    json.loads(creds_json),
    scopes=["https://www.googleapis.com/auth/drive"]
)

drive = build("drive", "v3", credentials=creds)
modelo = whisper.load_model("small")

# ========== FUNCIONES GOOGLE DRIVE ==========

def listar_archivos(carpeta_id):
    query = f"'{carpeta_id}' in parents and trashed = false"
    result = drive.files().list(q=query).execute()
    return result.get("files", [])

def descargar_archivo(file_id, filename):
    request = drive.files().get_media(fileId=file_id)
    fh = io.FileIO(filename, "wb")
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while not done:
        status, done = downloader.next_chunk()

def subir_archivo(nombre_local, carpeta_destino):
    file_metadata = {
        'name': os.path.basename(nombre_local),
        'parents': [carpeta_destino]
    }
    media = MediaIoBaseUpload(io.FileIO(nombre_local, "rb"), mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document")
    drive.files().create(body=file_metadata, media_body=media).execute()

def crear_carpeta(nombre, parent_id):
    metadata = {
        "name": nombre,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id]
    }
    carpeta = drive.files().create(body=metadata, fields="id").execute()
    return carpeta["id"]

# ========== TRANSCRIPCIÓN ==========

def hash_file(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        h.update(f.read())
    return h.hexdigest()

if os.path.exists(LOG_FILE):
    with open(LOG_FILE, "r") as f:
        procesados = json.load(f)
else:
    procesados = {}

archivos = listar_archivos(FOLDER_ID)
audios = [a for a in archivos if a["name"].lower().endswith(("mp3","m4a","wav","ogg","flac","aac"))]

nuevos = []
for a in audios:
    if a["id"] not in procesados:
        nuevos.append(a)

print(f"🔎 Audios nuevos encontrados: {len(nuevos)}")

if not nuevos:
    print("Nada nuevo para procesar.")
    exit()

for audio in tqdm(nuevos):
    nombre = audio["name"]
    temp_path = "temp_" + nombre
    descargar_archivo(audio["id"], temp_path)

    out = modelo.transcribe(temp_path, fp16=False)
    texto = out["text"].strip()

    fecha = datetime.date.today().isoformat()
    carpeta_dia = crear_carpeta(fecha, FOLDER_ID)

    doc = Document()
    doc.add_heading("Transcripción DGAGRO360°", 0)
    doc.add_paragraph("Archivo: " + nombre)
    doc.add_paragraph("Fecha procesado: " + fecha)
    doc.add_heading("Texto:", level=1)
    doc.add_paragraph(texto)

    salida = f"{nombre}_{fecha}.docx"
    doc.save(salida)

    subir_archivo(salida, carpeta_dia)

    procesados[audio["id"]] = {"nombre": nombre, "fecha": fecha}

    os.remove(temp_path)
    os.remove(salida)

with open(LOG_FILE, "w") as f:
    json.dump(procesados, f, indent=2)

print("✅ Procesado y subido a Google Drive correctamente.")

