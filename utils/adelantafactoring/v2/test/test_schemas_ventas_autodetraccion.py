"""
🧪 Test de Schemas VentasAutodetraccion V2
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Agregar el path correcto para importar
test_dir = os.path.dirname(__file__)
v2_dir = os.path.dirname(test_dir)
if v2_dir not in sys.path:
    sys.path.insert(0, v2_dir)


def test_ventas_autodetraccion_schemas():
    """Test de schemas VentasAutodetraccion V2"""

    # Import local
    from utils.adelantafactoring.v2.schemas.ventas_autodetraccion_schema import (
        VentasAutodetraccionesRequest,
        VentasAutodetraccionesResult,
    )

    # Crear DataFrames de prueba
    comprobantes_data = {
        "Estado Doc.Tributario": ["ACEPTADO", "ACEPTADO"],
        "Fecha Emisión ": ["01/01/2024", "02/01/2024"],
        "Tipo Documento": ["Factura", "Factura"],
        "Serie-Número ": ["F001-00001", "F001-00002"],
        "Ruc Cliente": ["12345678901", "12345678902"],
        "Cliente": ["Cliente 1", "Cliente 2"],
        "Op.Gravada": [1000.0, 2000.0],
        "Op. No Gravada": [0.0, 0.0],
        "IGV": [180.0, 360.0],
        "Importe Total": [1180.0, 2360.0],
        "Moneda": ["Sol", "Sol"],
    }
    comprobantes_df = pd.DataFrame(comprobantes_data)

    tipo_cambio_data = {
        "TipoCambioFecha": ["2024-01-01", "2024-01-02"],
        "TipoCambioCompra": [3.75, 3.76],
        "TipoCambioVenta": [3.77, 3.78],
    }
    tipo_cambio_df = pd.DataFrame(tipo_cambio_data)

    # Test VentasAutodetraccionesRequest
    request = VentasAutodetraccionesRequest(
        hasta="2024-01", comprobantes_df=comprobantes_df, tipo_cambio_df=tipo_cambio_df
    )

    assert request.hasta == "2024-01"
    assert len(request.comprobantes_df) == 2
    assert len(request.tipo_cambio_df) == 2

    # Test VentasAutodetraccionesResult
    from io import BytesIO

    buffer = BytesIO()

    result = VentasAutodetraccionesResult(
        excel_buffer=buffer,
        registro_ventas_count=10,
        autodetraccion_count=5,
        hasta="2024-01",
        total_autodetraccion=500.0,
    )

    assert result.registro_ventas_count == 10
    assert result.autodetraccion_count == 5
    assert result.hasta == "2024-01"
    assert result.total_autodetraccion == 500.0

    print("✅ VentasAutodetraccion Schemas V2: PASSED")


if __name__ == "__main__":
    test_ventas_autodetraccion_schemas()
