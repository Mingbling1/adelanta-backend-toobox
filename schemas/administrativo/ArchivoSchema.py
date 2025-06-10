from schemas.BaseSchema import BaseCreateSchema, BaseOutputSchema, BaseUpdateSchema
from uuid import UUID


class ArchivoCreateSchema(BaseCreateSchema):
    google_drive_archivo_id: str
    webViewLink: str
    name: str
    path: str
    path_id: str
    size: int
    content_type: str
    parent_folder_id: str
    gasto_id: UUID


class ArchivoOutputSchema(BaseOutputSchema):
    archivo_id: UUID
    webViewLink: str
    name: str
    path: str
    path_id: str
    size: int
    content_type: str
    parent_folder_id: str
    gasto_id: UUID


class ArchivoSearchOutputSchema(ArchivoOutputSchema):
    total_pages: int


class ArchivoUpdateSchema(BaseUpdateSchema):
    webViewLink: str
    name: str
    path: str
    path_id: str
    size: int
    content_type: str
    parent_folder_id: str
