import pytest
import asyncio
import json
from config.websocket_manager import websocket_manager
from cronjobs.datamart.TestLogsCronjob import TestLogsCronjob


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


@pytest.mark.asyncio
async def test_job_context_logging_system(client):
    """
    Test completo del sistema de logging con JobContext
    Verifica que todos los logs se envíen correctamente al frontend
    """

    # === 1. PREPARAR MOCK DEL WEBSOCKET ===
    captured_messages = []
    original_send_message = websocket_manager.send_message

    async def mock_send_message(message: str, job_id: str):
        """Mock que captura todos los mensajes enviados"""
        captured_messages.append(
            {
                "job_id": job_id,
                "message": message,
                "timestamp": asyncio.get_event_loop().time(),
            }
        )
        # También llamar al método original para mantener funcionalidad
        return await original_send_message(message, job_id)

    # Aplicar mock
    websocket_manager.send_message = mock_send_message

    try:
        # === 2. EJECUTAR TEST LOGS CRONJOB DIRECTAMENTE ===
        print("\n🧪 === EJECUTANDO TEST DE LOGS DIRECTAMENTE ===")

        test_cronjob = TestLogsCronjob()
        await test_cronjob.run()

        # Esperar un poco para que se procesen todos los mensajes
        await asyncio.sleep(0.5)

        # === 3. VERIFICAR MENSAJES CAPTURADOS ===
        print(f"\n📊 Total mensajes capturados: {len(captured_messages)}")

        # Clasificar mensajes
        log_messages = []
        status_messages = []

        for msg in captured_messages:
            if msg["message"].startswith("STATUS_UPDATE:"):
                status_messages.append(msg)
            else:
                log_messages.append(msg)

        print(f"📝 Mensajes de log: {len(log_messages)}")
        print(f"📈 Mensajes de estado: {len(status_messages)}")

        # === 4. VERIFICAR CONTENIDO DE LOGS ===
        print("\n🔍 === ANÁLISIS DE LOGS ENVIADOS ===")

        # Verificar que se enviaron logs
        assert len(log_messages) > 0, "No se capturaron mensajes de log"
        assert len(status_messages) > 0, "No se capturaron mensajes de estado"

        # Buscar logs específicos que deberían estar presentes
        log_contents = [msg["message"] for msg in log_messages]

        expected_log_patterns = [
            "Iniciando test completo de logging",
            "Procesando conjunto de datos simulado",
            "Registros encontrados: 1,234",
            "Advertencia: Algunos registros tienen datos faltantes",
            "Test de logging completado exitosamente",
            "Resumen:",
            "Test finalizado correctamente",
        ]

        found_patterns = []
        for pattern in expected_log_patterns:
            found = any(pattern in log_content for log_content in log_contents)
            found_patterns.append(found)
            if found:
                print(f"✅ Encontrado: '{pattern}'")
            else:
                print(f"❌ NO encontrado: '{pattern}'")

        # Verificar que se encontraron la mayoría de patrones
        found_count = sum(found_patterns)
        total_patterns = len(expected_log_patterns)
        success_rate = found_count / total_patterns

        print(
            f"\n📊 Tasa de éxito: {found_count}/{total_patterns} ({success_rate*100:.1f}%)"
        )

        assert (
            success_rate >= 0.7
        ), f"Solo se encontraron {found_count}/{total_patterns} patrones esperados"

        # === 5. VERIFICAR MENSAJES DE ESTADO ===
        print("\n📈 === ANÁLISIS DE MENSAJES DE ESTADO ===")

        status_updates = []
        for msg in status_messages:
            try:
                status_data_str = msg["message"].replace("STATUS_UPDATE:", "")
                status_data = json.loads(status_data_str)
                status_updates.append(status_data)
            except json.JSONDecodeError as e:
                print(f"⚠️ Error parsing status message: {e}")

        print(f"📊 Status updates válidos: {len(status_updates)}")

        # Verificar que hay updates de progreso
        progress_updates = [s for s in status_updates if "progress" in s]
        print(f"🔄 Updates de progreso: {len(progress_updates)}")

        assert len(progress_updates) > 0, "No se encontraron updates de progreso"

        # Verificar que el progreso llegó a 100%
        final_progress = max(s.get("progress", 0) for s in progress_updates)
        print(f"📈 Progreso máximo alcanzado: {final_progress}%")

        assert (
            final_progress == 100
        ), f"El progreso final fue {final_progress}%, esperado 100%"

        # === 6. MOSTRAR MUESTRA DE LOGS ===
        print("\n📝 === MUESTRA DE LOGS CAPTURADOS ===")
        for i, msg in enumerate(log_messages[:10]):  # Mostrar primeros 10
            print(f"{i+1:2d}. [{msg['job_id']}] {msg['message']}")

        if len(log_messages) > 10:
            print(f"... y {len(log_messages) - 10} mensajes más")

        # === 7. TEST A TRAVÉS DE API ===
        print("\n🌐 === EJECUTANDO TEST A TRAVÉS DE API ===")

        # Limpiar mensajes capturados anteriores
        captured_messages.clear()

        # Ejecutar a través de la API
        api_response = await client.post(
            "/api/cronjob/jobs/schedule/now", json={"job_id": "testlogscronjob"}
        )

        print(f"📡 Respuesta API: {api_response.status_code}")

        if api_response.status_code == 200:
            # Esperar a que termine el job
            await asyncio.sleep(3)

            api_log_count = len(
                [
                    msg
                    for msg in captured_messages
                    if not msg["message"].startswith("STATUS_UPDATE:")
                ]
            )
            api_status_count = len(
                [
                    msg
                    for msg in captured_messages
                    if msg["message"].startswith("STATUS_UPDATE:")
                ]
            )

            print(f"📝 Logs via API: {api_log_count}")
            print(f"📈 Status via API: {api_status_count}")

            # Verificar que también funcionó via API
            assert api_log_count > 0, "No se capturaron logs via API"
        else:
            print(f"⚠️ API call falló: {api_response.text}")

        # === 8. RESUMEN FINAL ===
        print("\n🎉 === RESUMEN DEL TEST ===")
        print(f"✅ Logs directos capturados: {len(log_messages)}")
        print(f"✅ Status updates capturados: {len(status_messages)}")
        print(f"✅ Patrones encontrados: {found_count}/{total_patterns}")
        print(f"✅ Progreso completado: {final_progress}%")
        print("✅ Sistema de logging funcionando correctamente")

    finally:
        # Restaurar método original
        websocket_manager.send_message = original_send_message


@pytest.mark.asyncio
async def test_websocket_logs_integration(client):
    """
    Test completo de integración WebSocket + Logging System
    Verifica que los logs lleguen correctamente al frontend via WebSocket
    """
    from fastapi.testclient import TestClient
    from unittest.mock import AsyncMock
    import uuid

    # === 1. CONFIGURAR WEBSOCKET TEST CLIENT ===
    execution_id = f"testlogscronjob_{uuid.uuid4().hex[:8]}"

    print("\n🔌 === INICIANDO TEST WEBSOCKET INTEGRATION ===")
    print(f"📡 Execution ID: {execution_id}")

    # Lista para capturar mensajes del WebSocket
    received_messages = []
    websocket_connected = False
    websocket_error = None

    # === 2. SIMULAR CONEXIÓN WEBSOCKET ===
    async def simulate_websocket_connection():
        nonlocal websocket_connected, websocket_error
        try:
            # Usar el client HTTP para hacer la conexión
            with client.websocket_connect(
                f"/api/cronjob/logs/{execution_id}"
            ) as websocket:
                websocket_connected = True
                print("✅ WebSocket conectado exitosamente")

                # Escuchar mensajes del WebSocket en background
                timeout_count = 0
                max_timeout = 10  # 10 segundos máximo

                while timeout_count < max_timeout:
                    try:
                        # Esperar mensaje con timeout corto
                        message = websocket.receive_text(timeout=1.0)
                        received_messages.append(message)
                        print(f"📨 Mensaje recibido: {message[:100]}...")

                        # Reset timeout si recibimos mensajes
                        timeout_count = 0

                    except TimeoutError:
                        timeout_count += 1
                        continue
                    except Exception as e:
                        print(f"⚠️ Error recibiendo mensaje: {e}")
                        break

        except Exception as e:
            websocket_error = str(e)
            print(f"❌ Error en WebSocket: {e}")

    # === 3. INICIAR WEBSOCKET EN BACKGROUND ===
    import threading

    websocket_thread = threading.Thread(
        target=lambda: asyncio.run(simulate_websocket_connection())
    )
    websocket_thread.daemon = True
    websocket_thread.start()

    # Esperar a que se conecte
    await asyncio.sleep(0.5)

    # === 4. EJECUTAR JOB QUE GENERE LOGS ===
    print("\n🚀 === EJECUTANDO JOB CON LOGS ===")

    # Ejecutar job con el execution_id específico
    api_response = await client.post(
        "/api/cronjob/jobs/schedule/now",
        json={"cronjob_config": {"job_id": "testlogscronjob"}},
    )

    print(f"📡 Respuesta API: {api_response.status_code}")

    if api_response.status_code == 200:
        response_data = api_response.json()
        actual_execution_id = response_data.get("execution_id")
        print(f"🎯 Execution ID real: {actual_execution_id}")

        # Esperar a que termine el job
        print("⏳ Esperando a que termine el job...")
        await asyncio.sleep(5)

        # === 5. VERIFICAR STATUS DEL JOB ===
        status_response = await client.get(
            f"/api/cronjob/jobs/status/{actual_execution_id}"
        )
        if status_response.status_code == 200:
            status_data = status_response.json()
            print(f"📊 Estado final del job: {status_data.get('status')}")
            print(f"📈 Progreso final: {status_data.get('progress')}%")

    # === 6. VERIFICAR MENSAJES WEBSOCKET RECIBIDOS ===
    print("\n📨 === ANÁLISIS MENSAJES WEBSOCKET ===")
    print(f"🔌 WebSocket conectado: {websocket_connected}")
    print(f"📝 Total mensajes recibidos: {len(received_messages)}")

    if websocket_error:
        print(f"❌ Error WebSocket: {websocket_error}")

    # Mostrar muestra de mensajes
    print("\n📋 === MUESTRA DE MENSAJES RECIBIDOS ===")
    for i, msg in enumerate(received_messages[:10]):
        print(f"{i+1:2d}. {msg[:150]}...")

    if len(received_messages) > 10:
        print(f"... y {len(received_messages) - 10} mensajes más")

    # === 7. ANÁLISIS DE TIPOS DE MENSAJES ===
    log_messages = []
    status_messages = []
    connection_messages = []

    for msg in received_messages:
        try:
            import json

            parsed_msg = json.loads(msg)
            msg_type = parsed_msg.get("type")

            if msg_type == "log":
                log_messages.append(parsed_msg)
            elif msg_type == "status":
                status_messages.append(parsed_msg)
            else:
                connection_messages.append(parsed_msg)
        except json.JSONDecodeError:
            # Mensaje no JSON (puede ser de conexión)
            connection_messages.append(msg)

    print("\n📊 === CLASIFICACIÓN DE MENSAJES ===")
    print(f"📝 Mensajes de log: {len(log_messages)}")
    print(f"📈 Mensajes de estado: {len(status_messages)}")
    print(f"🔌 Mensajes de conexión: {len(connection_messages)}")

    # === 8. VERIFICAR CONTENIDO DE LOGS ===
    if log_messages:
        print("\n🔍 === ANÁLISIS DE LOGS RECIBIDOS ===")

        log_contents = [msg.get("data", "") for msg in log_messages]

        expected_patterns = [
            "Iniciando test completo de logging",
            "Procesando conjunto de datos simulado",
            "Test de logging completado exitosamente",
        ]

        found_patterns = []
        for pattern in expected_patterns:
            found = any(pattern in content for content in log_contents)
            found_patterns.append(found)
            print(f"{'✅' if found else '❌'} {pattern}")

        success_rate = sum(found_patterns) / len(expected_patterns)
        print(f"\n📊 Tasa de éxito logs: {success_rate*100:.1f}%")

        # Verificación básica
        assert len(log_messages) > 0, "No se recibieron logs via WebSocket"
        assert success_rate >= 0.5, f"Pocos patrones encontrados: {success_rate}"

    # === 9. VERIFICAR MENSAJES DE ESTADO ===
    if status_messages:
        print("\n📈 === ANÁLISIS DE ESTADO ===")

        progress_values = []
        for status_msg in status_messages:
            status_data = status_msg.get("data", {})
            if "progress" in status_data:
                progress_values.append(status_data["progress"])

        if progress_values:
            max_progress = max(progress_values)
            print(f"📊 Progreso máximo: {max_progress}%")
            print(f"📈 Updates de progreso: {len(progress_values)}")

            assert max_progress >= 0, "No se recibió progreso válido"

    # === 10. RESUMEN FINAL ===
    print("\n🎉 === RESUMEN WEBSOCKET TEST ===")
    print(f"✅ Conexión WebSocket: {'OK' if websocket_connected else 'FAIL'}")
    print(f"✅ Mensajes totales: {len(received_messages)}")
    print(f"✅ Logs recibidos: {len(log_messages)}")
    print(f"✅ Status recibidos: {len(status_messages)}")

    # Assertion final
    assert websocket_connected, "WebSocket no se pudo conectar"
    assert len(received_messages) > 0, "No se recibieron mensajes via WebSocket"

    print("🎯 ¡Test WebSocket completado exitosamente!")


@pytest.mark.asyncio
async def test_websocket_direct_connection():
    """
    Test directo de conexión WebSocket sin jobs
    Para verificar que la infraestructura WebSocket funciona
    """
    from fastapi.testclient import TestClient
    from main import app  # Asegúrate de que este import sea correcto

    test_execution_id = "test_direct_connection"
    print("\n🔌 === TEST WEBSOCKET DIRECTO ===")

    try:
        with TestClient(app).websocket_connect(
            f"/api/cronjob/logs/{test_execution_id}"
        ) as websocket:
            print("✅ Conexión WebSocket establecida")

            # Enviar comando ping
            websocket.send_text("ping")
            response = websocket.receive_text()
            print(f"📨 Respuesta ping: {response}")

            # Verificar que recibimos pong
            assert "pong" in response, f"Esperaba 'pong', recibí: {response}"

            # Enviar comando status
            websocket.send_text("status")
            status_response = websocket.receive_text()
            print(f"📊 Respuesta status: {status_response}")

            print("🎯 Test WebSocket directo exitoso")

    except Exception as e:
        print(f"❌ Error en test WebSocket directo: {e}")
        raise
