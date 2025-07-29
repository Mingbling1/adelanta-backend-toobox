"""
🧪 Conftest V2 - Ultra simple y aislado
"""

import pytest

# Sin imports problemáticos, solo configuración básica de pytest


@pytest.fixture
def sample_data():
    """Datos de prueba simples"""
    return {"codigo": "TEST001", "monto": 1000.0, "moneda": "PEN"}
