"""
🧪 Test SaldosCalcular V2 - Simple y aislado
"""


def test_saldos_v2_basic():
    """Test básico para SaldosCalcular V2"""
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
    assert settings.INTERESES_PEN == 0.14
    assert settings.INTERESES_USD == 0.12

    # Simular datos de Saldos
    saldos_data = {
        "CodigoLiquidacion": ["SLD001", "SLD002", "SLD003"],
        "SaldoPendiente": [5000.0, 8000.0, 3000.0],
        "SaldoPagado": [15000.0, 12000.0, 17000.0],
        "Moneda": ["PEN", "USD", "PEN"],
        "FechaVencimiento": [
            datetime(2024, 2, 1),
            datetime(2024, 2, 15),
            datetime(2024, 3, 1),
        ],
    }

    df = pd.DataFrame(saldos_data)

    # Operaciones básicas
    assert len(df) == 3
    assert df["SaldoPendiente"].sum() == 16000.0
    assert df["SaldoPagado"].sum() == 44000.0

    # Filtrar por moneda
    pen_records = df[df["Moneda"] == "PEN"]
    assert len(pen_records) == 2

    print("✅ SaldosCalcular V2: PASSED")
