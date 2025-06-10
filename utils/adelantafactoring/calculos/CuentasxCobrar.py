import pandas as pd
import numpy as np

# Configuración inicial
archivo_excel = r"C:\Users\Jonathan\Desktop\colocaciones_acumulado_marzo_2025.xlsx"
fecha_corte = pd.to_datetime("2025-04-07")  # Adaptar según tu necesidad
tipo_de_cambio = 3.8  # Asumiendo un valor, actualizar si es dinámico

# Paso 1: Leer Excel (equivalente a "Origen" y "Navegación")
df = pd.read_excel(archivo_excel, sheet_name="Sheet1", engine='openpyxl')

# Paso 2: 
kpi_df = df
# Paso 3: Cambio de tipo de datos
conversiones = {
    "CodigoLiquidacion": str,
    "CodigoSolicitud": str,
    "RUCCliente": "Int64",
    "RazonSocialCliente": str,
    "RUCPagador": "Int64",
    "RazonSocialPagador": str,
    "Moneda": str,
    "DeudaAnterior": "Int64",
    "FlagPagoInteresConfirming": str,
    "FechaInteresConfirming": str,
    "TipoOperacion": str,
    "TipoOperacionDetalle": str,
    "Estado": str,
    "NroDocumento": str,
    "TasaNominalMensualPorc": float,
    "FinanciamientoPorc": "Int64",
    "FechaConfirmado": 'datetime64[ns]',
    "FechaOperacion": 'datetime64[ns]',
    "DiasEfectivo": "Int64",
    "NetoConfirmado": float,
    "FondoResguardo": float,
    "MontoComisionEstructuracion": float,
    "ComisionEstructuracionIGV": float,
    "ComisionEstructuracionConIGV": float,
    "MontoCobrar": float,
    "Interes": float,
    "InteresConIGV": float,
    "GastosContrato": "Int64",
    "GastoVigenciaPoder": "Int64",
    "ServicioCobranza": float,
    "ServicioCustodia": float,
    "GastosDiversosIGV": float,
    "GastosDiversosConIGV": float,
    "MontoTotalFacturado": float,
    "MontoDesembolso": float,
    "Ejecutivo": str,
    "FechaPago": 'datetime64[ns]',
    "FechaPagoCreacion": 'datetime64[ns]',
    "DiasMora": "Int64",
    "MontoCobrarPago": float,
    "MontoPago": float,
    "InteresPago": "Int64",
    "GastosPago": "Int64",
    "TipoPago": str,
    "SaldoDeuda": "Int64",
    "ExcesoPago": "Int64",
    "ObservacionPago": str,
    "FechaDesembolso": 'datetime64[ns]',
    "MontoDevolucion": float,
    "DescuentoDevolucion": "Int64",
    "EstadoDevolucion": str,
    "Anticipo": str
}
kpi_df = kpi_df.astype(conversiones, errors='ignore')

# Paso 4: Agregar columna "Saldo_Total"
kpi_df["Saldo_Total"] = np.where(
    kpi_df["TipoPago"] == "PAGO PARCIAL",
    kpi_df["SaldoDeuda"],
    np.where(kpi_df["TipoPago"] == "", kpi_df["NetoConfirmado"], kpi_df["NetoConfirmado"] - kpi_df["MontoPago"])
)

# Paso 5: Agregar columna "Saldo_Total_Pen"
kpi_df["Saldo_Total_Pen"] = np.where(
    kpi_df["Moneda"] == "PEN",
    kpi_df["Saldo_Total"],
    kpi_df["Saldo_Total"] * tipo_de_cambio
)

# Paso 6: Asegurar tipos
kpi_df["FechaOperacion"] = pd.to_datetime(kpi_df["FechaOperacion"], errors='coerce')
kpi_df["FechaConfirmado"] = pd.to_datetime(kpi_df["FechaConfirmado"], errors='coerce')

# Paso 7: Columna "TipoPago_Real"
lista_mora = [...]  # Copia la lista larga de códigos aquí
kpi_df["TipoPago_Real"] = kpi_df.apply(
    lambda row: "MORA A MAYO" if (row["TipoPago"] in ["PAGO PARCIAL", ""]) and (row["CodigoLiquidacion"] in lista_mora) else row["TipoPago"],
    axis=1
)

# Paso 8: Columna "Estado_Cuenta"
kpi_df["Estado_Cuenta"] = kpi_df["FechaConfirmado"].apply(lambda x: "VENCIDO" if pd.notnull(x) and x <= fecha_corte else "VIGENTE")

# Paso 9: Columna "Estado_Real"
lista_especial = [...]  # Copia la otra lista larga aquí
kpi_df["Estado_Real"] = kpi_df.apply(
    lambda row: "COBRANZA ESPECIAL" if row["Estado_Cuenta"] == "VENCIDO" and row["CodigoLiquidacion"] in lista_especial else row["Estado_Cuenta"],
    axis=1
)

# Resultado
kpi_df.head()  # Puedes exportar con kpi_df.to_excel("resultado.xlsx") si lo necesitas
