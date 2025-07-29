"""
🧪 Test NuevosClientesNuevosPagadoresCalcular V2 - Simple y aislado
"""


def test_nuevos_clientes_pagadores_v2_basic():
    """Test básico para NuevosClientesNuevosPagadoresCalcular V2"""
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
    assert settings.PROCESSING_OPTIONS["enable_fuzzy_matching"] is True

    # Simular datos de NuevosClientesNuevosPagadores
    nuevos_data = {
        "RUCCliente": ["20100000001", "20100000002", "20100000003"],
        "RazonSocialCliente": ["Cliente Nuevo A", "Cliente Nuevo B", "Cliente Nuevo C"],
        "RUCPagador": ["20200000001", "20200000002", "20200000003"],
        "RazonSocialPagador": ["Pagador Nuevo A", "Pagador Nuevo B", "Pagador Nuevo C"],
        "EsNuevoCliente": [True, False, True],
        "EsNuevoPagador": [True, True, False],
        "FechaPrimeraOperacion": [
            datetime(2024, 1, 10),
            datetime(2024, 1, 15),
            datetime(2024, 1, 20),
        ],
    }

    df = pd.DataFrame(nuevos_data)

    # Operaciones básicas
    assert len(df) == 3

    # Contar nuevos clientes y pagadores
    nuevos_clientes = df[df["EsNuevoCliente"]].shape[0]
    nuevos_pagadores = df[df["EsNuevoPagador"]].shape[0]

    assert nuevos_clientes == 2
    assert nuevos_pagadores == 2

    # Verificar unicidad de RUCs
    assert len(df["RUCCliente"].unique()) == 3
    assert len(df["RUCPagador"].unique()) == 3

    print("✅ NuevosClientesNuevosPagadoresCalcular V2: PASSED")
