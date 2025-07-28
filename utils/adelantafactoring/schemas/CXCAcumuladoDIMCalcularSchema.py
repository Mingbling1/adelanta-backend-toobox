from pydantic import BaseModel, Field, field_validator
from datetime import date, datetime
from typing import Optional
import pandas as pd
import numpy as np


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
    ObservacionLiquidacion: Optional[str] = Field(
        None, description="Observación de liquidación"
    )
    ObservacionSolicitud: Optional[str] = Field(
        None, description="Observación de solicitud"
    )
    FlagPagoInteresConfirming: str = Field(
        ..., description="Flag de pago interés confirming"
    )
    FechaInteresConfirming: Optional[date] = Field(
        None, description="Fecha interés confirming"
    )
    TipoOperacion: str = Field(..., description="Tipo de operación")
    TipoOperacionDetalle: str = Field(..., description="Detalle tipo de operación")
    Estado: str = Field(..., description="Estado")
    NroDocumento: str = Field(..., description="Número de documento")
    TasaNominalMensualPorc: float = Field(
        ..., description="Tasa nominal mensual porcentaje"
    )
    FinanciamientoPorc: float = Field(..., description="Financiamiento porcentaje")
    FechaConfirmado: date = Field(..., description="Fecha confirmado")
    FechaOperacion: date = Field(..., description="Fecha de operación")
    DiasEfectivo: int = Field(..., description="Días efectivo")
    NetoConfirmado: float = Field(..., description="Neto confirmado")
    FondoResguardo: float = Field(..., description="Fondo resguardo")
    ComisionEstructuracionPorc: float = Field(
        ..., description="Comisión estructuración porcentaje"
    )
    MontoComisionEstructuracion: float = Field(
        ..., description="Monto comisión estructuración"
    )
    ComisionEstructuracionIGV: float = Field(
        ..., description="Comisión estructuración IGV"
    )
    ComisionEstructuracionConIGV: float = Field(
        ..., description="Comisión estructuración con IGV"
    )
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
        "DeudaAnterior",
        "TasaNominalMensualPorc",
        "FinanciamientoPorc",
        "NetoConfirmado",
        "FondoResguardo",
        "ComisionEstructuracionPorc",
        "MontoComisionEstructuracion",
        "ComisionEstructuracionIGV",
        "ComisionEstructuracionConIGV",
        "MontoCobrar",
        "Interes",
        "InteresIGV",
        "InteresConIGV",
        "GastosContrato",
        "GastoVigenciaPoder",
        "ServicioCobranza",
        "ServicioCustodia",
        "GastosDiversos",
        "GastosDiversosIGV",
        "GastosDiversosConIGV",
        "MontoTotalFacturado",
        "MontoDesembolso",
        mode="before",
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
                "%d/%m/%Y",  # 15/10/2019
                "%Y-%m-%d",  # 2019-10-15
                "%d-%m-%Y",  # 15-10-2019
                "%Y/%m/%d",  # 2019/10/15
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
    def validate_fechas_requeridas(cls, v, info):
        """Convierte fechas requeridas - MANEJA PANDAS NaT"""

        field_name = info.field_name if info else "fecha_desconocida"

        # CASOS PROBLEMÁTICOS DE PANDAS
        if pd.isna(v) or v is pd.NaT:
            raise ValueError(
                f"ALERTA: {field_name} está vacía (NaT) - esto NO debería pasar en datos financieros"
            )

        if isinstance(v, (np.integer, np.floating)) and np.isnan(v):
            raise ValueError(
                f"ALERTA: {field_name} es NaN - esto NO debería pasar en datos financieros"
            )

        if v is None or v == "" or v == "null":
            raise ValueError(
                f"ALERTA: {field_name} está vacía - esto NO debería pasar en datos financieros"
            )

        if isinstance(v, date):
            return v

        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, str):
            formats = [
                "%d/%m/%Y",  # 15/10/2019
                "%Y-%m-%d",  # 2019-10-15
                "%d-%m-%Y",  # 15-10-2019
                "%Y/%m/%d",  # 2019/10/15
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

            raise ValueError(f"Formato de fecha no reconocido para {field_name}: {v}")

        raise ValueError(f"Tipo de fecha no soportado para {field_name}: {type(v)}")

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
        }


class CXCAcumuladoDIMCalcularSchema(BaseModel):
    # Campos originales
    IdLiquidacionCab: int = Field(..., description="ID de liquidación cabecera")
    IdLiquidacionDet: int = Field(..., description="ID de detalle de liquidación")

    # === CAMPOS PARA OPERACIONES FUERA DEL SISTEMA ===
    # Solo se usan para operaciones con FueraSistema == "si"
    # Se generan artificialmente para vincular con tablas CXCPagosFact y CXCDevFact
    IdLiquidacionPago: Optional[int] = Field(
        None, description="ID artificial de liquidación de pago (solo FueraSistema)"
    )
    IdLiquidacionDevolucion: Optional[int] = Field(
        None,
        description="ID artificial de liquidación de devolución (solo FueraSistema)",
    )

    CodigoLiquidacion: str = Field(..., description="Código de liquidación")
    CodigoSolicitud: str = Field(..., description="Código de solicitud")
    RUCCliente: str = Field(..., description="RUC del cliente")
    RazonSocialCliente: str = Field(..., description="Razón social del cliente")
    RUCPagador: str = Field(..., description="RUC del pagador")
    RazonSocialPagador: str = Field(..., description="Razón social del pagador")
    Moneda: str = Field(..., description="Moneda")
    DeudaAnterior: float = Field(..., description="Deuda anterior")
    ObservacionLiquidacion: Optional[str] = Field(
        None, description="Observación de liquidación"
    )
    ObservacionSolicitud: Optional[str] = Field(
        None, description="Observación de solicitud"
    )
    FlagPagoInteresConfirming: str = Field(
        ..., description="Flag de pago interés confirming"
    )
    FechaInteresConfirming: Optional[date] = Field(
        None, description="Fecha interés confirming"
    )
    TipoOperacion: str = Field(..., description="Tipo de operación")
    TipoOperacionDetalle: str = Field(..., description="Detalle tipo de operación")
    Estado: str = Field(..., description="Estado")
    NroDocumento: str = Field(..., description="Número de documento")
    TasaNominalMensualPorc: float = Field(
        ..., description="Tasa nominal mensual porcentaje"
    )
    FinanciamientoPorc: float = Field(..., description="Financiamiento porcentaje")
    FechaConfirmado: date = Field(..., description="Fecha confirmado")
    FechaOperacion: date = Field(..., description="Fecha de operación")
    DiasEfectivo: int = Field(..., description="Días efectivo")
    NetoConfirmado: float = Field(..., description="Neto confirmado")
    FondoResguardo: float = Field(..., description="Fondo resguardo")
    ComisionEstructuracionPorc: float = Field(
        ..., description="Comisión estructuración porcentaje"
    )
    MontoComisionEstructuracion: float = Field(
        ..., description="Monto comisión estructuración"
    )
    ComisionEstructuracionIGV: float = Field(
        ..., description="Comisión estructuración IGV"
    )
    ComisionEstructuracionConIGV: float = Field(
        ..., description="Comisión estructuración con IGV"
    )
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

    # === CAMPOS ETL KPI ===
    Mes: Optional[str] = Field(None, description="Mes en formato YYYY-MM")
    Año: Optional[str] = Field(None, description="Año de la operación")
    MesAño: Optional[str] = Field(None, description="Mes-Año en formato Enero-2024")
    MesSemana: Optional[str] = Field(None, description="Semana del mes")
    TipoCambioFecha: Optional[date] = Field(
        None, description="Fecha del tipo de cambio"
    )
    TipoCambioCompra: Optional[float] = Field(None, description="Tipo de cambio compra")
    TipoCambioVenta: Optional[float] = Field(None, description="Tipo de cambio venta")
    ColocacionSoles: Optional[float] = Field(None, description="Colocación en soles")
    MontoDesembolsoSoles: Optional[float] = Field(
        None, description="Monto desembolso en soles"
    )
    Ingresos: Optional[float] = Field(None, description="Ingresos calculados")
    IngresosSoles: Optional[float] = Field(None, description="Ingresos en soles")
    CostosFondo: Optional[float] = Field(None, description="Costos de fondo")
    TotalIngresos: Optional[float] = Field(None, description="Total de ingresos")
    CostosFondoSoles: Optional[float] = Field(
        None, description="Costos de fondo en soles"
    )
    TotalIngresosSoles: Optional[float] = Field(
        None, description="Total ingresos en soles"
    )
    Utilidad: Optional[float] = Field(None, description="Utilidad calculada")
    FueraSistema: Optional[str] = Field(
        None, description="Indica si viene de fuera del sistema"
    )
    Referencia: Optional[str] = Field(None, description="Referencia del referido")

    # === CAMPOS ETL POWER BI ===
    SaldoTotal: Optional[float] = Field(None, description="Saldo total calculado")
    SaldoTotalPen: Optional[float] = Field(None, description="Saldo total en PEN")
    TipoPagoReal: Optional[str] = Field(None, description="Tipo de pago real calculado")
    EstadoCuenta: Optional[str] = Field(
        None, description="Estado de cuenta (VENCIDO/VIGENTE)"
    )
    EstadoReal: Optional[str] = Field(
        None, description="Estado real con cobranza especial"
    )
    Sector: Optional[str] = Field(None, description="Sector del pagador")
    GrupoEco: Optional[str] = Field(None, description="Grupo económico")

    # ========================================================================
    # FIELD VALIDATORS INTELIGENTES PARA CAMPOS KPI (PYDANTIC RUST)
    # ========================================================================

    @field_validator("IdLiquidacionCab", "IdLiquidacionDet", mode="before")
    @classmethod
    def validate_required_integer_fields(cls, v, info):
        """Convierte floats/strings a enteros para campos OBLIGATORIOS - NO acepta None"""
        field_name = info.field_name if info else "campo_desconocido"

        # Campos obligatorios NO pueden ser None
        if v is None:
            raise ValueError(f"Campo obligatorio '{field_name}' no puede ser None")

        if v == "" or v == "null":
            raise ValueError(f"Campo obligatorio '{field_name}' no puede estar vacío")

        if isinstance(v, int):
            return v

        if isinstance(v, float):
            # Si es un float que representa un entero (ej: 1678.0), convertir a int
            if v.is_integer():
                return int(v)
            else:
                # Si tiene decimales, redondear hacia abajo
                return int(v)

        if isinstance(v, str):
            # Limpiar string y convertir
            v = v.strip().replace(",", "")
            if v == "":
                raise ValueError(
                    f"Campo obligatorio '{field_name}' no puede estar vacío"
                )
            try:
                # Intentar como float primero, luego convertir a int
                float_val = float(v)
                return int(float_val)
            except ValueError:
                raise ValueError(
                    f"No se puede convertir '{v}' a entero para campo '{field_name}'"
                )

        # Fallback para otros tipos
        try:
            return int(float(v))
        except (ValueError, TypeError):
            raise ValueError(
                f"No se puede convertir '{v}' (tipo: {type(v)}) a entero para campo obligatorio '{field_name}'"
            )

    @field_validator("IdLiquidacionPago", "IdLiquidacionDevolucion", mode="before")
    @classmethod
    def validate_optional_id_fields_fuera_sistema(cls, v, info):
        """
        Valida campos de ID opcionales para operaciones fuera del sistema.

        IMPORTANTE: Estos campos solo se usan para operaciones con FueraSistema == "si"
        - Para operaciones normales: None/NaN es válido
        - Para operaciones fuera del sistema: se generan artificialmente
        """
        # 🔥 MANEJO DE NaN DE PANDAS - Permitido para estos campos especiales
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return None

        # None es válido para estos campos opcionales
        if v is None or v == "" or v == "null":
            return None

        # Si tiene valor, convertir a entero
        if isinstance(v, int):
            return v

        if isinstance(v, float):
            if v.is_integer():
                return int(v)
            else:
                return int(v)

        if isinstance(v, str):
            v = v.strip().replace(",", "")
            if v == "":
                return None
            try:
                float_val = float(v)
                return int(float_val)
            except ValueError:
                # Si no se puede convertir, retornar None en lugar de error
                # para mantener flexibilidad en operaciones fuera del sistema
                return None

        # Fallback
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None

    @field_validator("DiasEfectivo", mode="before")
    @classmethod
    def validate_optional_integer_fields(cls, v):
        """Convierte floats/strings a enteros para campos OPCIONALES - acepta None/vacío como 0"""
        if v is None or v == "" or v == "null":
            return 0

        if isinstance(v, int):
            return v

        if isinstance(v, float):
            # Si es un float que representa un entero (ej: 35.0), convertir a int
            if v.is_integer():
                return int(v)
            else:
                # Si tiene decimales, redondear hacia abajo
                return int(v)

        if isinstance(v, str):
            # Limpiar string y convertir
            v = v.strip().replace(",", "")
            if v == "":
                return 0
            try:
                # Intentar como float primero, luego convertir a int
                float_val = float(v)
                return int(float_val)
            except ValueError:
                return 0

        # Fallback para otros tipos
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return 0
        except (ValueError, TypeError):
            return 0

    @field_validator(
        "DeudaAnterior",
        "TasaNominalMensualPorc",
        "FinanciamientoPorc",
        "NetoConfirmado",
        "FondoResguardo",
        "ComisionEstructuracionPorc",
        "MontoComisionEstructuracion",
        "ComisionEstructuracionIGV",
        "ComisionEstructuracionConIGV",
        "MontoCobrar",
        "Interes",
        "InteresIGV",
        "InteresConIGV",
        "GastosContrato",
        "GastoVigenciaPoder",
        "ServicioCobranza",
        "ServicioCustodia",
        "GastosDiversos",
        "GastosDiversosIGV",
        "GastosDiversosConIGV",
        "MontoTotalFacturado",
        "MontoDesembolso",
        mode="before",
    )
    @classmethod
    def validate_numeric_fields(cls, v):
        """Convierte strings a float, maneja casos especiales como notación científica y pd.isna()"""
        # 🔥 MANEJO CRÍTICO DE NaN DE PANDAS - SOLUCIÓN DEFINITIVA
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return 0.0

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
                # Esto maneja automáticamente notación científica como "0E-17"
                return float(v)
            except ValueError:
                return 0.0

        return 0.0

    @field_validator("TipoCambioFecha", mode="before")
    @classmethod
    def validate_tipo_cambio_fecha(cls, v):
        """Convierte fechas del tipo de cambio"""
        if v is None or v == "" or v == "null":
            return None

        if isinstance(v, date):
            return v

        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, str):
            formats = [
                "%d/%m/%Y",  # 15/10/2019
                "%Y-%m-%d",  # 2019-10-15
                "%d-%m-%Y",  # 15-10-2019
                "%Y/%m/%d",  # 2019/10/15
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

            return None  # Si no puede convertir, retorna None

        return None

    @field_validator(
        "TipoCambioCompra",
        "TipoCambioVenta",
        "ColocacionSoles",
        "MontoDesembolsoSoles",
        "Ingresos",
        "IngresosSoles",
        "CostosFondo",
        "TotalIngresos",
        "CostosFondoSoles",
        "TotalIngresosSoles",
        "Utilidad",
        "SaldoTotal",
        "SaldoTotalPen",
        mode="before",
    )
    @classmethod
    def validate_numeric_kpi_fields(cls, v):
        """Convierte strings a float para campos KPI numéricos (ULTRA RÁPIDO CON RUST) - CON SOPORTE pd.isna()"""
        # 🔥 MANEJO CRÍTICO DE NaN DE PANDAS - SOLUCIÓN DEFINITIVA
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return 0.0

        if (
            v is None
            or v == ""
            or v == "null"
            or (isinstance(v, str) and v.strip() == "")
        ):
            return 0.0

        if isinstance(v, (int, float)):
            return float(v)

        if isinstance(v, str):
            # Limpiar string y convertir
            v = v.strip().replace(",", "").replace("$", "").replace("%", "")
            if v == "":
                return 0.0
            try:
                return float(v)
            except ValueError:
                return 0.0

        return 0.0

    @field_validator(
        "Mes",
        "Año",
        "MesAño",
        "MesSemana",
        "FueraSistema",
        "Referencia",
        "TipoPagoReal",
        "EstadoCuenta",
        "EstadoReal",
        "Sector",
        "GrupoEco",
        mode="before",
    )
    @classmethod
    def validate_text_kpi_fields(cls, v):
        """Convierte valores a string para campos de texto KPI (ULTRA RÁPIDO CON RUST) - CON SOPORTE pd.isna()"""
        # 🔥 MANEJO CRÍTICO DE NaN DE PANDAS - SOLUCIÓN DEFINITIVA
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return ""

        if v is None or v == "null":
            return ""

        if isinstance(v, str):
            return v.strip()

        return str(v) if v is not None else ""

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
                "%d/%m/%Y",  # 15/10/2019
                "%Y-%m-%d",  # 2019-10-15
                "%d-%m-%Y",  # 15-10-2019
                "%Y/%m/%d",  # 2019/10/15
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

            raise ValueError(
                f"Formato de fecha no reconocido: {v}. Formatos soportados: dd/mm/yyyy, yyyy-mm-dd"
            )

        raise ValueError(f"Tipo de fecha no soportado: {type(v)}")

    # ========================================================================
    # VALIDATORS ESPECÍFICOS PARA CAMPOS QUE FALLAN CON NaN
    # ========================================================================

    @field_validator("CodigoSolicitud", mode="before")
    @classmethod
    def validate_codigo_solicitud(cls, v):
        """Valida CodigoSolicitud - convierte NaN a string vacío"""
        # 🔥 MANEJO DE NaN DE PANDAS
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return ""

        if v is None or v == "null":
            return ""

        if isinstance(v, str):
            return v.strip()

        # Convertir otros tipos a string
        return str(v).strip()

    @field_validator("ObservacionLiquidacion", "ObservacionSolicitud", mode="before")
    @classmethod
    def validate_observaciones_opcionales(cls, v):
        """Valida observaciones opcionales - convierte NaN a None"""
        # 🔥 MANEJO DE NaN DE PANDAS
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return None

        if v is None or v == "null" or v == "":
            return None

        if isinstance(v, str):
            stripped = v.strip()
            return stripped if stripped else None

        # Convertir otros tipos a string
        return str(v).strip() if str(v).strip() else None

    @field_validator("FlagPagoInteresConfirming", mode="before")
    @classmethod
    def validate_flag_pago_interes(cls, v):
        """Valida FlagPagoInteresConfirming - campo obligatorio, convierte NaN a string vacío"""
        # 🔥 MANEJO DE NaN DE PANDAS
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return ""

        if v is None or v == "null":
            return ""

        if isinstance(v, str):
            return v.strip()

        # Convertir otros tipos a string
        return str(v).strip()

    @field_validator("TipoOperacionDetalle", mode="before")
    @classmethod
    def validate_tipo_operacion_detalle(cls, v):
        """Valida TipoOperacionDetalle - campo obligatorio, convierte NaN a string vacío"""
        # 🔥 MANEJO DE NaN DE PANDAS
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return ""

        if v is None or v == "null":
            return ""

        if isinstance(v, str):
            return v.strip()

        # Convertir otros tipos a string
        return str(v).strip()

    @field_validator("FechaInteresConfirming", mode="before")
    @classmethod
    def validate_fecha_interes_confirming_nan(cls, v):
        """Valida FechaInteresConfirming - campo opcional, convierte NaN a None"""
        # 🔥 MANEJO DE NaN DE PANDAS
        if pd.isna(v) or (isinstance(v, float) and np.isnan(v)):
            return None

        if v is None or v == "null" or v == "":
            return None

        if isinstance(v, date):
            return v

        if isinstance(v, datetime):
            return v.date()

        if isinstance(v, str):
            formats = [
                "%d/%m/%Y",  # 15/10/2019
                "%Y-%m-%d",  # 2019-10-15
                "%d-%m-%Y",  # 15-10-2019
                "%Y/%m/%d",  # 2019/10/15
            ]

            for fmt in formats:
                try:
                    return datetime.strptime(v, fmt).date()
                except ValueError:
                    continue

            # Si no puede convertir, retorna None en lugar de error
            return None

        # Si no puede convertir, retorna None
        return None

    class Config:
        from_attributes = True
        json_encoders = {
            date: lambda v: v.isoformat() if v else None,
        }
