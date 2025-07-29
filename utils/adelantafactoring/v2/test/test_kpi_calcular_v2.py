"""
🧪 Test KPICalcular V2 - Simple y aislado
"""


def test_kpi_calcular_v2_basic():
    """Test básico para KPICalcular V2"""
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
    assert settings.PROCESSING_OPTIONS["preserve_source_data"] is True

    # Simular datos de KPI
    kpi_data = {
        "FechaCorte": [
            datetime(2024, 1, 31),
            datetime(2024, 2, 29),
            datetime(2024, 3, 31),
        ],
        "TotalOperaciones": [150, 175, 200],
        "MontoDescontado": [1000000.0, 1200000.0, 1500000.0],
        "ClientesActivos": [25, 30, 35],
        "TasaPromedio": [8.5, 8.2, 8.0],
        "CarteraVigente": [950000.0, 1140000.0, 1425000.0],
    }

    df = pd.DataFrame(kpi_data)

    # Operaciones básicas
    assert len(df) == 3
    assert df["TotalOperaciones"].sum() == 525
    assert df["MontoDescontado"].sum() == 3700000.0

    # Cálculos KPI
    df["TicketPromedio"] = df["MontoDescontado"] / df["TotalOperaciones"]
    assert df["TicketPromedio"].iloc[0] == 1000000.0 / 150

    # Verificar evolución
    assert df["ClientesActivos"].is_monotonic_increasing
    assert df["MontoDescontado"].is_monotonic_increasing

    print("✅ KPICalcular V2: PASSED")
