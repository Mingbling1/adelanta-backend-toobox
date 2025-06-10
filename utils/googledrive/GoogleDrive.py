from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from fastapi import UploadFile
from config.settings import settings

SCOPES = ["https://www.googleapis.com/auth/drive"]
PARENT_FOLDER_ID: str = "0AKs9bFFAjlQJUk9PVA"


class GoogleDriveClient:
    def __init__(self):
        self.creds = self.authenticate_google_drive()
        self.drive_service = build("drive", "v3", credentials=self.creds)

    def authenticate_google_drive(self):
        creds = Credentials(
            None,
            token_uri="https://oauth2.googleapis.com/token",
            refresh_token=settings.GOOGLE_REFRESH_TOKEN,
            client_id=settings.GOOGLE_CLIENT_ID,
            client_secret=settings.GOOGLE_CLIENT_SECRET,
            scopes=SCOPES,
        )
        return creds

    def upload_file(
        self,
        file: UploadFile,
        folder_id: str,
        path: str,
        path_id: str,
        created_by: str | None,
    ):
        file_metadata = {"name": file.filename, "parents": [folder_id]}
        media = MediaIoBaseUpload(file.file, mimetype=file.content_type)
        uploaded_file = (
            self.drive_service.files()
            .create(
                body=file_metadata,
                media_body=media,
                fields="id,webViewLink",
                supportsAllDrives=True,
            )
            .execute()
        )
        return {
            "google_drive_archivo_id": uploaded_file.get("id"),
            "webViewLink": uploaded_file.get("webViewLink"),
            "name": file.filename,
            "path": f"{path}",
            "size": file.size,
            "content_type": file.content_type,
            "parent_folder_id": folder_id,
            "path_id": f"{path_id}",
            "created_by": created_by,
        }

    async def upload_files(
        self,
        files: list[UploadFile],
        folder_id: str,
        path: str,
        path_id: str,
        created_by: str | None,
    ):
        file_urls = []
        for file in files:
            file_url = self.upload_file(file, folder_id, path, path_id, created_by)
            file_urls.append(file_url)
        return file_urls

    def delete_file(self, file_id: str):
        self.drive_service.files().delete(
            fileId=file_id, supportsAllDrives=True
        ).execute()

    def delete_folder(self, folder_id: str):
        self.drive_service.files().delete(
            fileId=folder_id, supportsAllDrives=True
        ).execute()

    # def create_folder(
    #     self,
    #     folder_name: str,
    #     parent_folder_id: str,
    #     path: str,
    #     path_id: str,
    #     existing_folders_dict: dict,
    # ):
    #     # Construir el nuevo path y path_id
    #     new_path = f"{path}/{folder_name}"
    #     new_path_id = f"{path_id}/{parent_folder_id}"

    #     # Verificar si la carpeta ya existe en existing_folders_dict
    #     if new_path in existing_folders_dict:
    #         folder_id = existing_folders_dict[new_path]
    #     else:
    #         # Crear la carpeta en Google Drive
    #         file_metadata = {
    #             "name": folder_name,
    #             "mimeType": "application/vnd.google-apps.folder",
    #             "parents": [parent_folder_id],
    #         }
    #         folder = (
    #             self.drive_service.files()
    #             .create(body=file_metadata, fields="id", supportsAllDrives=True)
    #             .execute()
    #         )
    #         folder_id = folder.get("id")

    #         # Actualizar existing_folders_dict
    #         existing_folders_dict[new_path] = folder_id

    #     return folder_id, new_path, new_path_id

    def create_folder(self, folder_name: str, parent_folder_id: str):
        response = (
            self.drive_service.files()
            .list(
                pageSize=1000,
                q="name='{}' and '{}' in parents and mimeType='application/vnd.google-apps.folder'".format(
                    folder_name, parent_folder_id
                ),
                fields="nextPageToken, files(id, name, parents)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
                corpora="drive",
                driveId=f"{PARENT_FOLDER_ID}",
            )
            .execute()
        )
        folders = response.get("files", [])
        if folders:
            return folders[0].get("id"), folder_name

        file_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = (
            self.drive_service.files()
            .create(body=file_metadata, fields="id", supportsAllDrives=True)
            .execute()
        )
        return folder.get("id"), folder_name

    def create_folders(
        self, folder_names: list, parent_folder_id: str, existing_folders_dict: dict
    ):
        path = "/" + "/".join(folder_names)
        if path in existing_folders_dict:
            path_id = existing_folders_dict[path]
            parent_folder_id = path_id.split("/")[-1]
        else:
            path = ""
            path_id = ""
            for folder_name in folder_names:
                folder_id, folder_name = self.create_folder(
                    folder_name, parent_folder_id
                )
                parent_folder_id = folder_id
                path = f"{path}/{folder_name}"
                path_id = f"{path_id}/{folder_id}"
        return parent_folder_id, path, path_id

    # def create_folders(
    #     self, folder_names: list, parent_folder_id: str, existing_folders_dict: dict
    # ):
    #     path = ""
    #     path_id = ""
    #     for folder_name in folder_names:
    #         folder_id, path, path_id = self.create_folder(
    #             folder_name,
    #             parent_folder_id,
    #             path,
    #             path_id,
    #             existing_folders_dict,
    #         )
    #         parent_folder_id = (
    #             folder_id  # Actualizar el parent_folder_id para la siguiente iteración
    #         )
    #     return parent_folder_id, path, path_id
