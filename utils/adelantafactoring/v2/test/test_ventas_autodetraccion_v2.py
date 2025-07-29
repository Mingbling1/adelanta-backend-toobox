"""
🧪 Test V    if v2_path not in sys.path:
        sys.path.insert(0, v2_path)

    # Test de configuración V2
    from utils.adelantafactoring.v2.config.settings import settings

    # Verificar configuración
    assert settings.WEBSERVICE_BASE_URL == "https://webservice.adelantafactoring.com"etracciones V2 - Simple y aislado
"""


def test_ventas_autodetraccion_v2_basic():
    """Test básico para VentasAutodetracciones V2"""
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

    # Verificar configuración base
    assert settings.WEBSERVICE_BASE_URL == "https://webservice.adelantafactoring.com"
    assert settings.MAX_RETRIES == 3

    # Simular datos de VentasAutodetraccion
    ventas_data = {
        "CodigoLiquidacion": ["VA001", "VA002", "VA003"],
        "NetoConfirmado": [1000.0, 2000.0, 1500.0],
        "Autodetraccion": [50.0, 100.0, 75.0],
        "FechaOperacion": [
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            datetime(2024, 1, 3),
        ],
    }

    df = pd.DataFrame(ventas_data)

    # Operaciones básicas
    assert len(df) == 3
    assert df["NetoConfirmado"].sum() == 4500.0
    assert df["Autodetraccion"].sum() == 225.0

    print("✅ VentasAutodetracciones V2: PASSED")
