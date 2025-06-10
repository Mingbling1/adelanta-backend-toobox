import pytest
from config.logger import logger


@pytest.mark.asyncio
async def test_create_tabla_maestra_and_detalle(client):
    # Crear entrada en TablaMaestra
    tabla_maestra_data = {
        "tabla_nombre": "all",
        "tipo": "estado",
        "estado": 1,
        "created_by": "d14875fc441b4a9ba7306a05fed4e764",  # Asegúrate de usar un UUID válido aquí
    }
    logger.debug(f"Enviando datos de la tabla maestra: {tabla_maestra_data}")
    response = await client.post("/master/tablaMaestra", json=tabla_maestra_data)
    logger.debug(f"Respuesta del servidor: {response.status_code}, {response.text}")
    assert response.status_code == 200, response.text
    tabla_maestra_creada = response.json()

    # Crear entradas en TablaMaestraDetalle asociadas
    detalles = [
        {
            "codigo": 0,
            "valor": "inactivo",
            "descripcion": "estado pendiente",
            "tabla_maestra_id": tabla_maestra_creada["tabla_maestra_id"],
            "estado": 1,
            "created_by": "d14875fc441b4a9ba7306a05fed4e764",  # Asegúrate de usar un UUID válido aquí
        },
        {
            "codigo": 1,
            "valor": "activo",
            "descripcion": "estado activo",
            "tabla_maestra_id": tabla_maestra_creada["tabla_maestra_id"],
            "estado": 1,
            "created_by": "d14875fc441b4a9ba7306a05fed4e764",  # Asegúrate de usar un UUID válido aquí
        },
    ]

    for detalle in detalles:
        logger.debug(f"Enviando datos del detalle de la tabla maestra: {detalle}")
        response = await client.post("/master/tablaMaestraDetalle", json=detalle)
        logger.debug(f"Respuesta del servidor: {response.status_code}, {response.text}")
        assert response.status_code == 200, response.text
        detalle_creado = response.json()

        # Verificar que el detalle se creó correctamente
        assert (
            detalle_creado["tabla_maestra_id"]
            == tabla_maestra_creada["tabla_maestra_id"]
        )
