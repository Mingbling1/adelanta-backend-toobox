import pytest


@pytest.mark.asyncio
async def test_create_cronjob(client):
    # Primer conjunto de datos para programar el cronjob "actualizartipocambiocronjob"
    schedule_data_1 = {
        "day_of_week": "0-6",
        "times": [[7, 0], [11, 45]],
        "cronjob_config": {"job_id": "actualizartipocambiocronjob"},
    }

    response_1 = await client.post("/api/cronjob/jobs/schedule", json=schedule_data_1)

    assert response_1.status_code == 200, response_1.text
    response_data_1 = response_1.json()
    assert "message" in response_data_1
    assert (
        response_data_1["message"]
        == "Job actualizartipocambiocronjob scheduled successfully at specified times"
    )

    # Segundo conjunto de datos para programar el cronjob "actualizartablasreportescronjob"
    schedule_data_2 = {
        "day_of_week": "0-6",
        "times": [[8, 30], [12, 30], [18, 5]],
        "cronjob_config": {"job_id": "actualizartablasreportescronjob"},
    }

    response_2 = await client.post("/api/cronjob/jobs/schedule", json=schedule_data_2)

    assert response_2.status_code == 200, response_2.text
    response_data_2 = response_2.json()
    assert "message" in response_data_2
    assert (
        response_data_2["message"]
        == "Job actualizartablasreportescronjob scheduled successfully at specified times"
    )
    # Tercer conjunto de datos para programar el cronjob "actualizartablaretomacronjob"
    schedule_data_3 = {
        "day_of_week": "0-6",
        "times": [[7, 5]],
        "cronjob_config": {
            "job_id": "actualizartablaretomacronjob",
            "fecha_corte": None,
        },
    }

    response_3 = await client.post("/api/cronjob/jobs/schedule", json=schedule_data_3)

    assert response_3.status_code == 200, response_3.text
    response_data_3 = response_3.json()
    assert "message" in response_data_3
    assert (
        response_data_3["message"]
        == "Job actualizartablaretomacronjob scheduled successfully at specified times"
    )
    # Cuarto conjunto de datos para programar el cronjob "actualizartablakpiacumuladocronjob"
    
    schedule_data_4 = {
        "day_of_week": "1-5",  # 1-5 representa lunes a viernes
        "times": [[5, 0], [19, 0]],  # 5:00 AM y 7:00 PM
        "cronjob_config": {"job_id": "actualizartablakpiacumuladocronjob"},
    }

    response_4 = await client.post("/api/cronjob/jobs/schedule", json=schedule_data_4)

    assert response_4.status_code == 200, response_4.text
    response_data_4 = response_4.json()
    assert "message" in response_data_4
    assert (
        response_data_4["message"]
        == "Job actualizartablakpiacumuladocronjob scheduled successfully at specified times"
    )
    # Forzar al scheduler a revisar los jobs pendientes
    forced_response = await client.post("/api/cronjob/force-check")
    assert forced_response.status_code == 200, forced_response.text
    forced_data = forced_response.json()
    assert forced_data["message"] == "Scheduler triggered to check for jobs"
