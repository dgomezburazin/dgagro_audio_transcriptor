import os
import json
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ID de tu carpeta DGAGRO_escucha
FOLDER_ID = "1NUq38acTjxIuEvhPgUvFnFlGEOdw8X5i"

def get_drive_service():
    creds_json = os.environ["GDRIVE_KEY"]
    info = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        info,
        scopes=["https://www.googleapis.com/auth/drive"]
    )
    return build("drive", "v3", credentials=creds)

def listar_archivos_carpeta(folder_id):
    service = get_drive_service()
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)"
    ).execute()
    files = results.get("files", [])
    print(f"Encontrados {len(files)} archivos en la carpeta:")
    for f in files:
        print(f"- {f['name']}  ({f['mimeType']})  id={f['id']}")

if __name__ == "__main__":
    print("🔄 Probando acceso a Google Drive…")
    listar_archivos_carpeta(FOLDER_ID)
    print("✅ Fin de prueba.")


