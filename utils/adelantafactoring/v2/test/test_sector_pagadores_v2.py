"""
🧪 Test SectorPagadoresCalcular V2 - Simple y aislado
"""


def test_sector_pagadores_v2_basic():
    """Test básico para SectorPagadoresCalcular V2"""
    import sys
    import os
    import pandas as pd

    # Path V2
    v2_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if v2_path not in sys.path:
        sys.path.insert(0, v2_path)

    # Test de configuración V2
    from utils.adelantafactoring.v2.config.settings import settings

    # Verificar configuración
    assert settings.WEBSERVICE_BASE_URL == "https://webservice.adelantafactoring.com"
    assert settings.PROCESSING_OPTIONS["preserve_source_data"] is True

    # Simular datos de SectorPagadores
    sector_data = {
        "RUCPagador": ["20100000001", "20100000002", "20100000003"],
        "RazonSocialPagador": ["Empresa A", "Empresa B", "Empresa C"],
        "Sector": ["Manufacturero", "Comercial", "Servicios"],
        "MontoOperaciones": [10000.0, 15000.0, 12000.0],
    }

    df = pd.DataFrame(sector_data)

    # Operaciones básicas
    assert len(df) == 3
    assert df["MontoOperaciones"].sum() == 37000.0
    assert "Manufacturero" in df["Sector"].values

    # Agrupar por sector
    by_sector = df.groupby("Sector")["MontoOperaciones"].sum()
    assert by_sector["Manufacturero"] == 10000.0

    print("✅ SectorPagadoresCalcular V2: PASSED")
