from .BaseCalcular import BaseCalcular
from ..schemas.OperacionesFueraSistemaCalcularSchema import (
    OperacionesFueraSistemaCalcularSchema,
)
from config.logger import logger
from ..obtener.OperacionesFueraSistemaObtener import OperacionesFueraSistemaObtener
import pandas as pd


class OperacionesFueraSistemaCalcular(BaseCalcular):
    columnas = {
        "Liquidación": "CodigoLiquidacion",
        "N° Factura/ Letra": "NroDocumento",
        "Nombre Cliente": "RazonSocialCliente",
        "RUC Cliente": "RUCCliente",
        "Nombre Deudor": "RazonSocialPagador",
        "RUC DEUDOR": "RUCPagador",
        "TNM Op": "TasaNominalMensualPorc",
        "TNA Op": "",  # No hay una columna directamente correspondiente
        "% Finan": "FinanciamientoPorc",
        "Fecha de Op": "FechaOperacion",
        "F.Pago Confirmada": "FechaConfirmado",
        "Días Efect": "DiasEfectivo",
        "Moneda": "Moneda",
        "Neto Confirmado": "NetoConfirmado",
        "% Estructuracion": "",  # No hay una columna directamente correspondiente
        "Comisión de Estructuracion": "MontoComisionEstructuracion",
        "IGV Comisión": "ComisionEstructuracionIGV",
        "Comision Con IGV": "ComisionEstructuracionConIGV",
        "Fondo Resguardo": "FondoResguardo",
        "Neto a Financiar": "MontoCobrar",
        "Interés sin IGV": "Interes",
        "IGV Interés": "",  # No hay una columna directamente correspondiente
        "Interés con IGV": "InteresConIGV",
        "Contrato": "GastosContrato",
        "Servicio de custodia sin IGV": "ServicioCustodia",
        "Servicio de cobranza de documentos sin IGV": "ServicioCobranza",
        "Comisión por envío de carta notarial sin IGV": "GastoVigenciaPoder",
        "Gastos Diversos Sin IGV": "GastosDiversosSinIGV",
        "IGV Gastos Diversos": "GastosDiversosIGV",
        "Gastos Diversos con IGV": "GastosDiversosConIGV",
        "Total a ser facturado al desembolso Inc. IGV": "MontoTotalFacturado",
        "N Factura generada": "FacturasGeneradas",
        "Desembolso Neto": "MontoDesembolso",
        "Fecha de pago": "FechaPago",
        "Estado": "Estado",
        "Dias Mora": "DiasMora",
        "TNM Moratorio": "",  # No hay una columna directamente correspondiente
        "TNA Moratorio": "",  # No hay una columna directamente correspondiente
        "Interes Mora - No Afecto IGV": "InteresPago",
        "Factura Int Mora": "",  # No hay una columna directamente correspondiente
        "Gastos Mora Con IGV": "GastosPago",
        "Factura Gastos Mora Con IGV": "",  # No hay una columna directamente correspondiente
        "Importe a Pagar": "MontoCobrarPago",
        "Import. Recaudado": "MontoPago",
        "Excedente Generado a ser devuelto": "ExcesoPago",
        "Fecha de devolución de excedente": "FechaDesembolso",
        "Total factura Mora": "",  # No hay una columna directamente correspondiente
        "EJECUTIVO": "Ejecutivo",
        "TIPO DE OPERACIÓN": "TipoOperacion",
    }

    def __init__(self):
        super().__init__()
        self.operaciones_fuera_sistema_obtener = OperacionesFueraSistemaObtener()

    def validar_datos(self, data: list[dict]) -> list[dict]:
        # Convierte la lista de dicts en un DataFrame de pandas
        df = pd.DataFrame(data)
        # Filtra filas donde RUCCliente o RUCPagador sean NaN o cadenas vacías
        df = df.dropna(subset=["RUCCliente", "RUCPagador"])
        df = df[
            (df["RUCCliente"].astype(str).str.strip() != "")
            & (df["RUCPagador"].astype(str).str.strip() != "")
        ]
        data = df.to_dict(orient="records")
        # Retorna la lista de registros resultante
        try:
            datos_validados = [
                OperacionesFueraSistemaCalcularSchema(**item).model_dump()
                for item in data
            ]
            return datos_validados
        except Exception as e:
            logger.error(e)
            raise e

    def procesar_datos(self, data_pen: dict, data_usd: dict) -> list[dict]:
        df_pen = pd.DataFrame(data_pen)
        df_usd = pd.DataFrame(data_usd)
        df = pd.concat([df_pen, df_usd])
        df = df.rename(columns=self.columnas).drop(columns=[""])
        return df.to_dict(orient="records")

    def calcular(self) -> list[dict]:
        datos_procesados = self.procesar_datos(
            self.operaciones_fuera_sistema_obtener.obtener_operaciones_fuera_sistema_pen(),
            self.operaciones_fuera_sistema_obtener.obtener_operaciones_fuera_sistema_usd(),
        )
        datos_validados = self.validar_datos(datos_procesados)
        return datos_validados

    def calcular_df(self) -> pd.DataFrame:
        return pd.DataFrame(self.calcular())
