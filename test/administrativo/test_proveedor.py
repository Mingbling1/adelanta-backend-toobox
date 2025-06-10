import pytest
from migration import proveedores, cuentas_bancarias
from config.logger import logger


@pytest.mark.asyncio
async def test_create_proveedor_and_cuenta_bancaria(client):
    for proveedor in proveedores:
        # Crear proveedor
        proveedor_data = {
            "nombre_proveedor": proveedor["nombre_proveedor"],
            "tipo_proveedor": proveedor["tipo_proveedor"],
            "tipo_documento": proveedor["tipo_documento"],
            "numero_documento": proveedor["numero_documento"],
            "estado": proveedor["estado"],
            "created_by": proveedor["created_by"],
        }
        logger.debug(f"Enviando datos del proveedor: {proveedor_data}")
        response = await client.post("/administrativo/proveedor", json=proveedor_data)
        logger.debug(f"Respuesta del servidor: {response.status_code}, {response.text}")
        assert response.status_code == 200, response.text
        proveedor_creado = response.json()

        # Crear cuentas bancarias asociadas
        for cuenta_id in proveedor["cuenta_bancaria"]:
            cuenta_bancaria_data = next(
                cuenta for cuenta in cuentas_bancarias if cuenta["_id"] == cuenta_id
            )
            cuenta_bancaria_data["proveedor_id"] = proveedor_creado["proveedor_id"]
            logger.debug(
                f"Enviando datos de la cuenta bancaria: {cuenta_bancaria_data}"
            )
            response = await client.post(
                "/administrativo/cuentaBancaria", json=cuenta_bancaria_data
            )
            logger.debug(
                f"Respuesta del servidor: {response.status_code}, {response.text}"
            )
            assert response.status_code == 200, response.text
            cuenta_bancaria_creada = response.json()

            # Verificar que la cuenta bancaria se creó correctamente
            assert (
                cuenta_bancaria_creada["proveedor_id"]
                == proveedor_creado["proveedor_id"]
            )
