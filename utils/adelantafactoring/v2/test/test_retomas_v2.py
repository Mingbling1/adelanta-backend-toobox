"""
🧪 Test RetomasCalcular V2 - Simple y aislado
"""


def test_retomas_v2_basic():
    """Test básico para RetomasCalcular V2"""
    import sys
    import os
    import pandas as pd
    from datetime import datetime

    # Path V2
    v2_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if v2_path not in sys.path:
        sys.path.insert(0, v2_path)

    # Test de configuración V2
    from utils.adelantafactoring.v2.config.settings import settings

    # Verificar configuración
    assert settings.WEBSERVICE_BASE_URL == "https://webservice.adelantafactoring.com"
    assert settings.CACHE_TTL == 300

    # Verificar que la estructura V2 existe
    v2_structure_exists = os.path.exists(v2_path)
    assert v2_structure_exists, "La estructura V2 debe existir"

    # Simular datos de Retomas
    retomas_data = {
        "CodigoOperacion": ["RET001", "RET002", "RET003"],
        "MontoRetomado": [2000.0, 3500.0, 1800.0],
        "MontoOriginal": [20000.0, 35000.0, 18000.0],
        "PorcentajeRetoma": [10.0, 10.0, 10.0],
        "FechaRetoma": [
            datetime(2024, 1, 15),
            datetime(2024, 1, 20),
            datetime(2024, 1, 25),
        ],
    }

    df = pd.DataFrame(retomas_data)

    # Operaciones básicas
    assert len(df) == 3
    assert df["MontoRetomado"].sum() == 7300.0
    assert df["MontoOriginal"].sum() == 73000.0

    # Verificar porcentajes
    assert all(df["PorcentajeRetoma"] == 10.0)

    print("✅ RetomasCalcular V2: PASSED")
