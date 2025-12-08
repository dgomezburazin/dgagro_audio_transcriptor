# ==========================================================
# 🌾 DG|AGRO360° – Transcriptor Profesional de Audios (vFinal AutoMail con Fecha por Nombre)
# ==========================================================
!pip -q install openai-whisper python-docx pydub tqdm unidecode

import os, json, re, hashlib, datetime, shutil
from tqdm import tqdm
from docx import Document
from pydub import AudioSegment
from collections import Counter
import whisper
import smtplib
from email.mime.text import MIMEText

# ==========================================================
# ⚙️ CONFIGURACIÓN
# ==========================================================
CARPETA_BASE = "/content/drive/MyDrive/DGAGRO_escucha"
CARPETA_TRANSCRIPCIONES = os.path.join(CARPETA_BASE, "transcripciones")
os.makedirs(CARPETA_TRANSCRIPCIONES, exist_ok=True)
LOG_PATH = os.path.join(CARPETA_BASE, ".processed_log.json")
MEMORIA_CAMPOS = os.path.join(CARPETA_BASE, "diccionario_campos.json")
FECHA_HOY = datetime.date.today().isoformat()

# 📧 CONFIGURACIÓN DE EMAIL
SENDER = "gomezd136@gmail.com"
PASSWORD = "agrogbcjdtxufwep"   # clave de aplicación (16 caracteres)
RECIPIENT = "damiangomez.agro360@gmail.com"
URL_CARPETA_BASE = "https://drive.google.com/drive/folders/1dZ4IXTj1xucBpXME5pnS95KgE0Sv0TXn?usp=drive_link"

# ==========================================================
# 🧩 FUNCIONES AUXILIARES
# ==========================================================
def cargar_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                contenido = f.read().strip()
                if not contenido:
                    return default
                return json.loads(contenido)
        except json.JSONDecodeError:
            print(f"⚠️ {os.path.basename(path)} vacío o dañado. Se reinicia.")
            return default
    return default

def guardar_json(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

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
    except:
        return None

# ==========================================================
# 🧠 DETECCIÓN DE NOMBRE DE CAMPO / LOTE
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
# 📄 CREAR WORD INDIVIDUAL
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

# ==========================================================
# 📘 CREAR WORD MAESTRO (versión incremental)
# ==========================================================
def crear_docx_maestro(items, salida):
    """
    Crea o actualiza un documento Word maestro con todas las transcripciones del día.
    Si el archivo ya existe, se agrega contenido nuevo sin borrar lo previo.
    """
    from docx.shared import Pt
    from docx.oxml.ns import qn
    from docx.oxml import OxmlElement

    if os.path.exists(salida):
        doc = Document(salida)
        doc.add_paragraph("")  # Espacio antes del nuevo bloque
        doc.add_paragraph("──────────────────────────────", style=None)
        doc.add_paragraph(f"🆕 Nuevas transcripciones agregadas el {FECHA_HOY}")
        print(f"📎 Archivo existente detectado → se agregan nuevas transcripciones: {os.path.basename(salida)}")
    else:
        doc = Document()
        doc.add_heading("Lomas_Pampeanas – Compilado General de Transcripciones", 0)
        doc.add_paragraph(f"Actualizado al {FECHA_HOY}")
        doc.add_paragraph("")
        print(f"📄 Creando nuevo compilado maestro: {os.path.basename(salida)}")

    # Agrupar nuevos ítems por campo detectado
    agrupados = {}
    for it in items:
        agrupados.setdefault(it["campo_detectado"], []).append(it)

    # Agregar cada campo y sus audios al documento
    for campo, lista in agrupados.items():
        doc.add_heading(f"📍 {campo}", level=1)
        for it in sorted(lista, key=lambda x: x["fecha_archivo"]):
            doc.add_heading(f"🎧 Audio – {it['fecha_archivo']}", level=2)
            doc.add_paragraph(it["texto"])
            doc.add_paragraph("")

    # Guardar cambios
    doc.save(salida)
    print(f"📘 Word maestro actualizado correctamente: {salida}")


# ==========================================================
# 🎙️ PROCESAMIENTO PRINCIPAL
# ==========================================================
from google.colab import drive

# Robust Google Drive mount logic
MOUNT_POINT = '/content/drive'

# Check if the mount point exists and is a directory
if os.path.exists(MOUNT_POINT) and os.path.isdir(MOUNT_POINT):
    # Check if it's already a mount point
    if not os.path.ismount(MOUNT_POINT):
        # If it's not a mount point but contains files, it's in an unexpected state.
        # Clearing its contents will resolve the "Mountpoint must not already contain files" error.
        if len(os.listdir(MOUNT_POINT)) > 0:
            print(f"WARNING: {MOUNT_POINT} exists and contains files but is not a mount point. Clearing its contents...")
            # Remove all contents of the directory (not the directory itself)
            for item in os.listdir(MOUNT_POINT):
                item_path = os.path.join(MOUNT_POINT, item)
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.remove(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            print(f"Contents of {MOUNT_POINT} cleared.")
else:
    # If the mount point directory does not exist, create it.
    os.makedirs(MOUNT_POINT, exist_ok=True)

drive.mount(MOUNT_POINT, force_remount=True)

modelo = whisper.load_model("small")
log = cargar_json(LOG_PATH, {"procesados": {}})
memoria_campos = cargar_json(MEMORIA_CAMPOS, {})

exts = (".mp3",".m4a",".wav",".ogg",".flac",".aac")
todos = [f for f in os.listdir(CARPETA_BASE) if f.lower().endswith(exts)]

nuevos = []
for nombre in todos:
    path = os.path.join(CARPETA_BASE, nombre)
    h = hash_archivo(path)
    if h not in log["procesados"]:
        nuevos.append((path, nombre, h))

if not nuevos:
    print("✅ No hay audios nuevos para procesar.")
else:
    print(f"🔎 Audios nuevos encontrados: {len(nuevos)}")

resumen_items = []
for path_audio, nombre, h in tqdm(nuevos, desc="🎧 Transcribiendo"):
    info = os.stat(path_audio)

    # 🗓️ Detección robusta de fecha: desde el nombre o desde el sistema
    fecha_nombre = re.search(r"(\d{4}[-_]\d{2}[-_]\d{2})", nombre)
    if fecha_nombre:
        fecha_archivo = fecha_nombre.group(1)
    else:
        fecha_archivo = datetime.datetime.fromtimestamp(info.st_mtime).strftime("%Y-%m-%d")

    dur_min = duracion_min(path_audio)
    out = modelo.transcribe(path_audio, fp16=False)
    texto = out.get("text","").strip()
    campo_detectado = detectar_nombre_campo(texto, memoria_campos)
    meta = {
        "nombre": nombre,
        "fecha_archivo": fecha_archivo,
        "duracion_min": dur_min,
        "campo_detectado": campo_detectado,
        "texto": texto
    }
    nombre_final = f"{campo_detectado.replace(' ', '_')}_{fecha_archivo}.docx"
    salida_docx = os.path.join(CARPETA_TRANSCRIPCIONES, nombre_final)
    crear_docx_audio(salida_docx, meta, texto)
    resumen_items.append(meta)
    log["procesados"][h] = {"nombre": nombre, "campo": campo_detectado, "fecha": fecha_archivo, "texto": texto}

guardar_json(LOG_PATH, log)
guardar_json(MEMORIA_CAMPOS, memoria_campos)

# ==========================================================
# 📁 SUBCARPETAS POR FECHA REAL + WORDS + ENVÍO EMAIL
# ==========================================================
if resumen_items:
    agrupados_por_fecha = {}
    for it in resumen_items:
        fecha_dia = it["fecha_archivo"]
        agrupados_por_fecha.setdefault(fecha_dia, []).append(it)

    subcarpetas_creadas = []

    for fecha_dia, lista_dia in agrupados_por_fecha.items():
        carpeta_dia = os.path.join(CARPETA_TRANSCRIPCIONES, fecha_dia)
        os.makedirs(carpeta_dia, exist_ok=True)

        for it in lista_dia:
            campo = it["campo_detectado"].replace(" ", "_")
            nombre_docx = f"{campo}_{fecha_dia}.docx"
            src = os.path.join(CARPETA_TRANSCRIPCIONES, nombre_docx)
            dst = os.path.join(carpeta_dia, nombre_docx)
            if os.path.exists(src):
                shutil.move(src, dst)

        salida_dia = os.path.join(carpeta_dia, f"Transcripciones_Completas_{fecha_dia}.docx")
        crear_docx_maestro(lista_dia, salida_dia)
        subcarpetas_creadas.append((fecha_dia, carpeta_dia, salida_dia))

        print(f"\n✅ Transcripciones del {fecha_dia} guardadas en: {carpeta_dia}")
        print(f"📘 Resumen diario: {salida_dia}")

else:
    print("\nℹ️ No se generó nada nuevo.")

# ==========================================================
# ✉️ EMAIL SIMPLE Y ROBUSTO (solo carpeta principal)
# ==========================================================
if resumen_items and subcarpetas_creadas:

    url_base = "https://drive.google.com/drive/folders/1UneuMrWOAiirDIFWLYaQe5oyUrfBU9ZG?usp=drive_link"

    cuerpo_detalles = ""
    for fecha_dia, carpeta_path, salida_dia in subcarpetas_creadas:
        maestro_nombre = os.path.basename(salida_dia)
        cuerpo_detalles += (
            f"📅 {fecha_dia}\n"
            f"   ✔ Subcarpeta creada en Drive\n"
            f"   ✔ Word maestro: {maestro_nombre}\n\n"
        )

    cuerpo = f"""
Hola Damian,

Las transcripciones fueron procesadas correctamente.

📁 Podés acceder a TODAS las fechas aquí:
{url_base}

Detalles generados hoy:

{cuerpo_detalles}

Dentro de la carpeta encontrarás:
- Subcarpetas ordenadas por fecha
- Documentos individuales por audio
- Documento maestro por día

Saludos,
DG|AGRO360°
"""

    msg = MIMEText(cuerpo, _charset="utf-8")
    msg["Subject"] = "DG|AGRO360° – Nuevas transcripciones procesadas"
    msg["From"] = SENDER
    msg["To"] = RECIPIENT

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SENDER, PASSWORD)
            server.sendmail(SENDER, [RECIPIENT], msg.as_string())
        print("📨 Email enviado correctamente.")
    except Exception as e:
        print("⚠️ Error al enviar correo:", e)
