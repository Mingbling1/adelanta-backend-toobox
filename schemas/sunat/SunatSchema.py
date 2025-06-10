from pydantic import BaseModel, Field
from typing import Literal, Union


class EmpresaSchema(BaseModel):
    razonSocial: str
    tipoDocumento: str
    numeroDocumento: str
    estado: str
    condicion: str
    direccion: str
    ubigeo: str
    viaTipo: str
    viaNombre: str
    zonaCodigo: str
    zonaTipo: str
    numero: str
    interior: str
    lote: str
    dpto: str
    manzana: str
    kilometro: str
    distrito: str
    provincia: str
    departamento: str
    EsAgenteRetencion: bool
    tipo: str
    actividadEconomica: str
    numeroTrabajadores: str
    tipoFacturacion: str
    tipoContabilidad: str
    comercioExterior: str


class PersonaSchema(BaseModel):
    nombres: str
    apellidoPaterno: str
    apellidoMaterno: str
    tipoDocumento: str
    numeroDocumento: str
    digitoVerificador: str


class PersonaTipoSchema(BaseModel):
    sunat_tipo: Literal["dni"]
    dni: str


class EmpresaTipoSchema(BaseModel):
    sunat_tipo: Literal["ruc"]
    ruc: str


class SunatTipoSchema(BaseModel):
    tipo: Union[EmpresaTipoSchema, PersonaTipoSchema] = Field(
        ..., discriminator="sunat_tipo"
    )
