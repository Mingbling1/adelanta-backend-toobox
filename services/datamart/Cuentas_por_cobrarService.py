import pandas as pd
import numpy as np

class Cuentas_por_Cobrar:
    def __init__(self, fecha_corte: str, tipo_de_cambio: float):
        self.fecha_corte = pd.to_datetime(fecha_corte)
        self.tipo_de_cambio = tipo_de_cambio
        self.df = None

    def cargar_dataframe(self, df: pd.DataFrame):
        self.df = df.copy()

    def convertir_tipos(self):
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
        self.df = self.df.astype(conversiones, errors='ignore')

    def agregar_saldo_total(self):
        self.df["Saldo_Total"] = np.where(
            self.df["TipoPago"] == "PAGO PARCIAL",
            self.df["SaldoDeuda"],
            np.where(self.df["TipoPago"] == "", self.df["NetoConfirmado"], self.df["NetoConfirmado"] - self.df["MontoPago"])
        )

    def agregar_saldo_total_pen(self):
        self.df["Saldo_Total_Pen"] = np.where(
            self.df["Moneda"] == "PEN",
            self.df["Saldo_Total"],
            self.df["Saldo_Total"] * self.tipo_de_cambio
        )

    def agregar_tipo_pago_real(self, lista_mora):
        self.df["TipoPago_Real"] = self.df.apply(
            lambda row: "MORA A MAYO" if (row["TipoPago"] in ["PAGO PARCIAL", ""]) and (row["CodigoLiquidacion"] in lista_mora) else row["TipoPago"],
            axis=1
        )

    def agregar_estado_cuenta(self):
        self.df["Estado_Cuenta"] = self.df["FechaConfirmado"].apply(
            lambda x: "VENCIDO" if pd.notnull(x) and x <= self.fecha_corte else "VIGENTE"
        )

    def agregar_estado_real(self, lista_especial):
        self.df["Estado_Real"] = self.df.apply(
            lambda row: "COBRANZA ESPECIAL" if row["Estado_Cuenta"] == "VENCIDO" and row["CodigoLiquidacion"] in lista_especial else row["Estado_Cuenta"],
            axis=1
        )

    def procesar(self, lista_mora, lista_especial):
        self.convertir_tipos()
        self.agregar_saldo_total()
        self.agregar_saldo_total_pen()
        self.agregar_tipo_pago_real(lista_mora)
        self.agregar_estado_cuenta()
        self.agregar_estado_real(lista_especial)
        return self.df
