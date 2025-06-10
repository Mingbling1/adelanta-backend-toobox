from config.db_mysql_crm import BaseCRM
from models.BaseModel import BaseModel


class CRMBaseModel(BaseCRM, BaseModel):
    """
    Clase base para modelos CRM.
    Hereda de BaseCRM y BaseModel para incluir campos comunes.
    """

    __abstract__ = True  # Indica que esta clase no debe ser instanciada directamente

    # Aquí puedes agregar campos o métodos comunes a todos los modelos CRM
    # Ejemplo: campos de auditoría, estado, etc.

    # Si necesitas agregar campos específicos, puedes hacerlo aquí
    # Por ejemplo:
    # nombre: Mapped[str] = mapped_column(String(255), nullable=False)
