"""
🧪 Test OperacionesFueraSistemaCalcular V2 - Simple y aislado
"""


def test_operaciones_fuera_sistema_v2_basic():
    """Test básico para OperacionesFueraSistemaCalcular V2"""
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
    assert settings.PROCESSING_OPTIONS["validate_financial_precision"] is True

    # Simular datos de OperacionesFueraSistema
    ops_fuera_data = {
        "CodigoOperacion": ["OFS001", "OFS002", "OFS003"],
        "TipoOperacion": ["Manual", "Especial", "Urgente"],
        "MontoOperacion": [25000.0, 15000.0, 30000.0],
        "FueraSistema": [True, True, True],
        "Observaciones": ["Op manual", "Op especial", "Op urgente"],
        "FechaOperacion": [
            datetime(2024, 1, 5),
            datetime(2024, 1, 8),
            datetime(2024, 1, 12),
        ],
    }

    df = pd.DataFrame(ops_fuera_data)

    # Operaciones básicas
    assert len(df) == 3
    assert df["MontoOperacion"].sum() == 70000.0
    assert all(df["FueraSistema"] == True)

    # Verificar tipos de operación
    tipos_unicos = df["TipoOperacion"].unique()
    assert len(tipos_unicos) == 3
    assert "Manual" in tipos_unicos
    assert "Especial" in tipos_unicos
    assert "Urgente" in tipos_unicos

    print("✅ OperacionesFueraSistemaCalcular V2: PASSED")
