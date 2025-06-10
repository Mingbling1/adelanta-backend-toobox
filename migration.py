import pandas as pd

# Cargar los archivos CSV
proveedor_df = pd.read_csv(r"C:\Users\Mingbling\Documents/toolbox.proveedor.csv")
cuenta_bancaria_df = pd.read_csv(
    r"C:\Users\Mingbling\Documents/toolbox.cuenta_bancaria.csv"
)

# Proveedor
proveedor_df["cuentas"] = proveedor_df.apply(
    lambda row: [
        cuenta for cuenta in [row["cuentas[0]"], row["cuentas[1]"]] if pd.notna(cuenta)
    ],
    axis=1,
)

proveedor_df = proveedor_df.drop(columns=["cuentas[0]", "cuentas[1]"])
proveedor_df = proveedor_df.loc[
    :,
    [
        "_id",
        "nombreProveedor",
        "tipoProveedor",
        "tipoDocumento",
        "numeroDocumento",
        "cuentas",
    ],
].rename(
    columns={
        "nombreProveedor": "nombre_proveedor",
        "tipoProveedor": "tipo_proveedor",
        "tipoDocumento": "tipo_documento",
        "numeroDocumento": "numero_documento",
    }
)
proveedor_df = proveedor_df.rename(columns={"cuentas": "cuenta_bancaria"})
proveedor_df["numero_documento"] = proveedor_df["numero_documento"].astype(str)
proveedor_df["estado"] = 1
proveedor_df["created_by"] = "d14875fc441b4a9ba7306a05fed4e764"
proveedores = proveedor_df.to_dict(orient="records")

# Cuenta Bancaria
cuenta_bancaria_df = cuenta_bancaria_df.loc[
    :, ["_id", "banco", "moneda", "tipoCuenta", "cc", "cci", "nota"]
].rename(
    columns={
        "tipoCuenta": "tipo_cuenta",
    }
)
cuenta_bancaria_df["estado"] = 1
cuenta_bancaria_df["nota"] = cuenta_bancaria_df["nota"].apply(
    lambda x: "" if pd.isna(x) else x
)

cuenta_bancaria_df["created_by"] = "d14875fc441b4a9ba7306a05fed4e764"
cuentas_bancarias = cuenta_bancaria_df.to_dict(orient="records")


###############################################################

# Crear una lista para almacenar las filas combinadas
combined_rows = []

# Combinar los datos
for proveedor in proveedores:
    for cuenta_id in proveedor["cuenta_bancaria"]:
        cuenta_bancaria = next(
            (cuenta for cuenta in cuentas_bancarias if cuenta["_id"] == cuenta_id), None
        )
        if cuenta_bancaria:
            combined_row = {**proveedor, **cuenta_bancaria}
            combined_rows.append(combined_row)

# Convertir la lista de filas combinadas a un DataFrame
combined_df = pd.DataFrame(combined_rows)

combined_df.to_excel("proveedores_cuentas_bancarias.xlsx", index=False)


###########################################
# Cargar los archivos CSV
gasto_df = pd.read_csv(r"C:\Users\Mingbling\Documents\toolbox.gasto.csv")
pago_df = pd.read_csv(r"C:\Users\Mingbling\Documents\toolbox.pago.csv")

# Transformar los datos de gasto
gasto_df["pagos"] = gasto_df.apply(
    lambda row: [pago for pago in [row["pagos[0]"]] if pd.notna(pago)],
    axis=1,
)

gasto_df = gasto_df.drop(columns=["pagos[0]"])
gasto_df = gasto_df.loc[
    :,
    [
        "_id",
        "tipoGasto",
        "files[0]",
        "files[1]",
        "proveedor",
        "tipoCDP",
        "numeroCDP",
        "fechaEmision",
        "importe",
        "moneda",
        "tipoDescuento",
        "estado",
        "motivo",
        "naturalezaGasto",
        "centroCostos",
        "fechaPagoTentativa",
        "interruptorNulos",
        "interruptorFecha",
        "fechaContable",
        "filesPath",
        "filesPathId",
        "filesParentFolderId",
        "createdBy",
        "createdAt",
        "montoNeto",
        "updatedAt",
        "updatedBy",
        "pagos",
    ],
].rename(
    columns={
        "tipoGasto": "tipo_gasto",
        "files[0]": "files_0",
        "files[1]": "files_1",
        "tipoCDP": "tipo_cdp",
        "numeroCDP": "numero_cdp",
        "fechaEmision": "fecha_emision",
        "tipoDescuento": "tipo_descuento",
        "naturalezaGasto": "naturaleza_gasto",
        "centroCostos": "centro_costos",
        "fechaPagoTentativa": "fecha_pago_tentativa",
        "interruptorNulos": "interruptor_nulos",
        "interruptorFecha": "interruptor_fecha",
        "fechaContable": "fecha_contable",
        "filesPath": "files_path",
        "filesPathId": "files_path_id",
        "filesParentFolderId": "files_parent_folder_id",
        "createdBy": "created_by",
        "createdAt": "created_at",
        "montoNeto": "monto_neto",
        "updatedAt": "updated_at",
        "updatedBy": "updated_by",
    }
)

# Transformar los datos de pago
pago_df = pago_df.loc[
    :, ["_id", "pagoFecha", "pagoMonto", "pagoEstado", "proveedorCuenta", "createdBy", "createdAt", "updatedAt", "updatedBy"]
].rename(
    columns={
        "pagoFecha": "pago_fecha",
        "pagoMonto": "pago_monto",
        "pagoEstado": "pago_estado",
        "proveedorCuenta": "proveedor_cuenta",
        "createdBy": "created_by",
        "createdAt": "created_at",
        "updatedAt": "updated_at",
        "updatedBy": "updated_by",
    }
)

# Convertir a diccionarios
gastos = gasto_df.to_dict(orient="records")
pagos = pago_df.to_dict(orient="records")

# Crear una lista para almacenar las filas combinadas
combined_rows = []

# Combinar los datos
for gasto in gastos:
    if gasto["pagos"]:
        for pago_id in gasto["pagos"]:
            pago = next((p for p in pagos if p["_id"] == pago_id), None)
            if pago:
                combined_row = {**gasto, **pago}
            else:
                combined_row = {**gasto, "pago_fecha": None, "pago_monto": None, "pago_estado": None, "proveedor_cuenta": None, "created_by": None, "created_at": None, "updated_at": None, "updated_by": None}
            combined_rows.append(combined_row)
    else:
        combined_row = {**gasto, "pago_fecha": None, "pago_monto": None, "pago_estado": None, "proveedor_cuenta": None, "created_by": None, "created_at": None, "updated_at": None, "updated_by": None}
        combined_rows.append(combined_row)

# Convertir la lista de filas combinadas a un DataFrame
combined_df = pd.DataFrame(combined_rows)
combined_df.to_excel("gastos_pagos.xlsx", index=False)