from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional



class CXCAcumuladoDIMRawSchema(BaseModel):
    """
    Schema para validar datos RAW que vienen del webservice.
    Convierte automáticamente strings a tipos apropiados.
    """
    # Campos originales que vienen del webservice
    IdLiquidacionCab: int = Field(..., description="ID de liquidación cabecera")
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    CodigoLiquidacion: str = Field(..., description="Código de liquidación")
    CodigoSolicitud: str = Field(..., description="Código de solicitud")
    RUCCliente: str = Field(..., description="RUC del cliente")
    RazonSocialCliente: str = Field(..., description="Razón social del cliente")
    RUCPagador: str = Field(..., description="RUC del pagador")
    RazonSocialPagador: str = Field(..., description="Razón social del pagador")
    Moneda: str = Field(..., description="Moneda")
    DeudaAnterior: float = Field(..., description="Deuda anterior")
    ObservacionLiquidacion: Optional[str] = Field(None, description="Observación de liquidación")
    ObservacionSolicitud: Optional[str] = Field(None, description="Observación de solicitud")
    FlagPagoInteresConfirming: str = Field(..., description="Flag de pago interés confirming")
    FechaInteresConfirming: Optional[date] = Field(None, description="Fecha interés confirming")
    TipoOperacion: str = Field(..., description="Tipo de operación")
    TipoOperacionDetalle: str = Field(..., description="Detalle tipo de operación")
    Estado: str = Field(..., description="Estado")
    NroDocumento: str = Field(..., description="Número de documento")
    TasaNominalMensualPorc: float = Field(..., description="Tasa nominal mensual porcentaje")
    FinanciamientoPorc: float = Field(..., description="Financiamiento porcentaje")
    FechaConfirmado: date = Field(..., description="Fecha confirmado")
    FechaOperacion: date = Field(..., description="Fecha de operación")
    DiasEfectivo: int = Field(..., description="Días efectivo")
    NetoConfirmado: float = Field(..., description="Neto confirmado")
    FondoResguardo: float = Field(..., description="Fondo resguardo")
    ComisionEstructuracionPorc: float = Field(..., description="Comisión estructuración porcentaje")
    MontoComisionEstructuracion: float = Field(..., description="Monto comisión estructuración")
    ComisionEstructuracionIGV: float = Field(..., description="Comisión estructuración IGV")
    ComisionEstructuracionConIGV: float = Field(..., description="Comisión estructuración con IGV")
    FacturasGeneradas: Optional[str] = Field(None, description="Facturas generadas")
    MontoCobrar: float = Field(..., description="Monto a cobrar")
    Interes: float = Field(..., description="Interés")
    InteresIGV: float = Field(..., description="Interés IGV")
    InteresConIGV: float = Field(..., description="Interés con IGV")
    GastosContrato: float = Field(..., description="Gastos de contrato")
    GastoVigenciaPoder: float = Field(..., description="Gasto vigencia poder")
    ServicioCobranza: float = Field(..., description="Servicio cobranza")
    ServicioCustodia: float = Field(..., description="Servicio custodia")
    GastosDiversos: float = Field(..., description="Gastos diversos")
    GastosDiversosIGV: float = Field(..., description="Gastos diversos IGV")
    GastosDiversosConIGV: float = Field(..., description="Gastos diversos con IGV")
    MontoTotalFacturado: float = Field(..., description="Monto total facturado")
    MontoDesembolso: float = Field(..., description="Monto desembolso")
    Ejecutivo: str = Field(..., description="Ejecutivo")

    @field_validator(
        "DeudaAnterior", "TasaNominalMensualPorc", "FinanciamientoPorc", 
        "NetoConfirmado", "FondoResguardo", "ComisionEstructuracionPorc",
        "MontoComisionEstructuracion", "ComisionEstructuracionIGV", 
        "ComisionEstructuracionConIGV", "MontoCobrar", "Interes", 
        "InteresIGV", "InteresConIGV", "GastosContrato", "GastoVigenciaPoder",
        "ServicioCobranza", "ServicioCustodia", "GastosDiversos", 
        "GastosDiversosIGV", "GastosDiversosConIGV", "MontoTotalFacturado", 
        "MontoDesembolso", mode="before"
    )
    @classmethod
    def validate_numeric_fields(cls, v):
        """Convierte strings a float, maneja casos especiales"""
        if v is None or v == "" or v == "null":
            return 0.0
        
        if isinstance(v, (int, float)):
            return float(v)
        
        if isinstance(v, str):
            # Limpiar string y convertir
            v = v.strip().replace(",", "")
            if v == "":
                return 0.0
            try:
                return float(v)
            except ValueError:
                return 0.0
        
        return 0.0

    @field_validator("FechaInteresConfirming", mode="before")
    @classmethod
    def validate_fecha_interes_confirming(cls, v):
        """Convierte fechas en formato dd/mm/yyyy a date object"""
        if v is None or v == "" or v == "null":
            return None
        
        if isinstance(v, date):
            return v
        
        if isinstance(v, datetime):
            return v.date()
        
        if isinstance(v, str):
            formats = [
                "%d/%m/%Y",    # 15/10/2019
                "%Y-%m-%d",    # 2019-10-15
                "%d-%m-%Y",    # 15-10-2019
                "%Y/%m/%d",    # 2019/10/15
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
            
            raise ValueError(f"Formato de fecha no reconocido: {v}")
        
        raise ValueError(f"Tipo de fecha no soportado: {type(v)}")

    @field_validator("FechaConfirmado", "FechaOperacion", mode="before")
    @classmethod
    def validate_fechas_requeridas(cls, v):
        """Convierte fechas requeridas en formato dd/mm/yyyy a date object"""
        if v is None or v == "" or v == "null":
            raise ValueError("Esta fecha es requerida")
        
        if isinstance(v, date):
            return v
        
        if isinstance(v, datetime):
            return v.date()
        
        if isinstance(v, str):
            formats = [
                "%d/%m/%Y",    # 15/10/2019
                "%Y-%m-%d",    # 2019-10-15
                "%d-%m-%Y",    # 15-10-2019
                "%Y/%m/%d",    # 2019/10/15
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
            
            raise ValueError(f"Formato de fecha no reconocido: {v}")
        
        raise ValueError(f"Tipo de fecha no soportado: {type(v)}")

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
        }

class CXCAcumuladoDIMCalcularSchema(BaseModel):
    # Campos originales
    IdLiquidacionCab: int = Field(..., description="ID de liquidación cabecera")
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")
    CodigoLiquidacion: str = Field(..., description="Código de liquidación")
    CodigoSolicitud: str = Field(..., description="Código de solicitud")
    RUCCliente: str = Field(..., description="RUC del cliente")
    RazonSocialCliente: str = Field(..., description="Razón social del cliente")
    RUCPagador: str = Field(..., description="RUC del pagador")
    RazonSocialPagador: str = Field(..., description="Razón social del pagador")
    Moneda: str = Field(..., description="Moneda")
    DeudaAnterior: float = Field(..., description="Deuda anterior")
    ObservacionLiquidacion: Optional[str] = Field(None, description="Observación de liquidación")
    ObservacionSolicitud: Optional[str] = Field(None, description="Observación de solicitud")
    FlagPagoInteresConfirming: str = Field(..., description="Flag de pago interés confirming")
    FechaInteresConfirming: Optional[date] = Field(None, description="Fecha interés confirming")
    TipoOperacion: str = Field(..., description="Tipo de operación")
    TipoOperacionDetalle: str = Field(..., description="Detalle tipo de operación")
    Estado: str = Field(..., description="Estado")
    NroDocumento: str = Field(..., description="Número de documento")
    TasaNominalMensualPorc: float = Field(..., description="Tasa nominal mensual porcentaje")
    FinanciamientoPorc: float = Field(..., description="Financiamiento porcentaje")
    FechaConfirmado: date = Field(..., description="Fecha confirmado")
    FechaOperacion: date = Field(..., description="Fecha de operación")
    DiasEfectivo: int = Field(..., description="Días efectivo")
    NetoConfirmado: float = Field(..., description="Neto confirmado")
    FondoResguardo: float = Field(..., description="Fondo resguardo")
    ComisionEstructuracionPorc: float = Field(..., description="Comisión estructuración porcentaje")
    MontoComisionEstructuracion: float = Field(..., description="Monto comisión estructuración")
    ComisionEstructuracionIGV: float = Field(..., description="Comisión estructuración IGV")
    ComisionEstructuracionConIGV: float = Field(..., description="Comisión estructuración con IGV")
    FacturasGeneradas: Optional[str] = Field(None, description="Facturas generadas")
    MontoCobrar: float = Field(..., description="Monto a cobrar")
    Interes: float = Field(..., description="Interés")
    InteresIGV: float = Field(..., description="Interés IGV")
    InteresConIGV: float = Field(..., description="Interés con IGV")
    GastosContrato: float = Field(..., description="Gastos de contrato")
    GastoVigenciaPoder: float = Field(..., description="Gasto vigencia poder")
    ServicioCobranza: float = Field(..., description="Servicio cobranza")
    ServicioCustodia: float = Field(..., description="Servicio custodia")
    GastosDiversos: float = Field(..., description="Gastos diversos")
    GastosDiversosIGV: float = Field(..., description="Gastos diversos IGV")
    GastosDiversosConIGV: float = Field(..., description="Gastos diversos con IGV")
    MontoTotalFacturado: float = Field(..., description="Monto total facturado")
    MontoDesembolso: float = Field(..., description="Monto desembolso")
    Ejecutivo: str = Field(..., description="Ejecutivo")

    # === CAMPOS ETL CALCULADOS ===
    SaldoTotal: float = Field(..., description="Saldo total calculado")
    SaldoTotalPen: float = Field(..., description="Saldo total en PEN")
    TipoPagoReal: str = Field(..., description="Tipo de pago real calculado")
    EstadoCuenta: str = Field(..., description="Estado de cuenta (VENCIDO/VIGENTE)")
    EstadoReal: str = Field(..., description="Estado real con cobranza especial")
    Sector: Optional[str] = Field(None, description="Sector del pagador")
    GrupoEco: Optional[str] = Field(None, description="Grupo económico")

    @field_validator("FechaInteresConfirming", mode="before")
    @classmethod
    def validate_fecha_interes_confirming(cls, v):
        """Convierte fechas en formato dd/mm/yyyy a date object"""
        if v is None or v == "" or v == "null":
            return None
        
        if isinstance(v, date):
            return v
        
        if isinstance(v, datetime):
            return v.date()
        
        if isinstance(v, str):
            formats = [
                "%d/%m/%Y",    # 15/10/2019
                "%Y-%m-%d",    # 2019-10-15
                "%d-%m-%Y",    # 15-10-2019
                "%Y/%m/%d",    # 2019/10/15
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue
            
            raise ValueError(f"Formato de fecha no reconocido: {v}. Formatos soportados: dd/mm/yyyy, yyyy-mm-dd")
        
        raise ValueError(f"Tipo de fecha no soportado: {type(v)}")

    # @field_validator("FechaConfirmado", "FechaOperacion", mode="before")
    # @classmethod
    # def validate_fechas_requeridas(cls, v):
    #     """Convierte fechas requeridas en formato dd/mm/yyyy a date object"""
    #     if v is None or v == "" or v == "null":
    #         raise ValueError("Esta fecha es requerida")
        
    #     if isinstance(v, date):
    #         return v
        
    #     if isinstance(v, datetime):
    #         return v.date()
        
    #     if isinstance(v, str):
    #         formats = [
    #             "%d/%m/%Y",    # 15/10/2019
    #             "%Y-%m-%d",    # 2019-10-15
    #             "%d-%m-%Y",    # 15-10-2019
    #             "%Y/%m/%d",    # 2019/10/15
    #         ]
            
    #         for fmt in formats:
    #             try:
    #                 return datetime.strptime(v, fmt).date()
    #             except ValueError:
    #                 continue
            
    #         raise ValueError(f"Formato de fecha no reconocido: {v}. Formatos soportados: dd/mm/yyyy, yyyy-mm-dd")
        
    #     raise ValueError(f"Tipo de fecha no soportado: {type(v)}")

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
        }