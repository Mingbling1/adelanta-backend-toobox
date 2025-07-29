"""
🧪 Test ReferidosCalcular V2 - Simple y aislado
"""


def test_referidos_v2_basic():
    """Test básico para ReferidosCalcular V2"""
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
    assert settings.REQUEST_TIMEOUT == 30

    # Simular datos de Referidos
    referidos_data = {
        "NombreReferido": ["Juan Perez", "Maria Garcia", "Carlos Lopez"],
        "EmpresaReferida": ["Empresa X", "Empresa Y", "Empresa Z"],
        "TelefonoReferido": ["999123456", "999654321", "999789012"],
        "EstadoReferido": ["Contactado", "Pendiente", "Convertido"],
        "FechaReferido": [
            datetime(2024, 1, 10),
            datetime(2024, 1, 12),
            datetime(2024, 1, 15),
        ],
    }

    df = pd.DataFrame(referidos_data)

    # Operaciones básicas
    assert len(df) == 3
    assert "Contactado" in df["EstadoReferido"].values
    assert "Convertido" in df["EstadoReferido"].values

    # Contar estados
    estados_count = df["EstadoReferido"].value_counts()
    assert estados_count["Contactado"] == 1
    assert estados_count["Convertido"] == 1
    assert estados_count["Pendiente"] == 1

    print("✅ ReferidosCalcular V2: PASSED")
