# 🚀 V2 Quick Reference - Cheat Sheet

## 📋 **¿Dónde colocar cada archivo?**

```bash
# 📤 DESDE V1 → V2
XxxCalcular.py    → engines/calculation/xxx_calculation_engine.py
XxxObtener.py     → io/webservice/xxx_client.py
XxxSchema.py      → schemas/xxx_schema.py

# 🔧 NUEVOS COMPONENTES V2
                  → processing/transformers/xxx_transformer.py
                  → processing/validators/xxx_validator.py
                  → api/xxx_api.py (interfaz pública)
                  → test/test_xxx.py
```

## ⚡ **Templates Rápidos**

### 🌐 API (Interfaz Pública)

```python
# api/xxx_api.py
def get_xxx(fecha_corte: str = None) -> List[Dict]:
    """Función pública simple"""
    client = XxxWebserviceClient()
    engine = XxxCalculationEngine()
    transformer = XxxTransformer()

    raw_data = client.fetch_data(fecha_corte)
    processed = engine.calculate(raw_data)
    return transformer.to_list_dict(processed)
```

### ⚙️ ENGINE (Motor de Cálculo)

```python
# engines/calculation/xxx_calculation_engine.py
class XxxCalculationEngine:
    """Motor especializado para cálculos Xxx"""

    def calculate(self, data: pd.DataFrame) -> pd.DataFrame:
        """Lógica matemática/financiera principal"""
        # Implementar lógica desde XxxCalcular.py
```

### 📡 IO CLIENT (Comunicación Externa)

```python
# io/webservice/xxx_client.py
from utils.adelantafactoring.v2.config.settings import settings

class XxxWebserviceClient:
    """Cliente para datos Xxx"""

    def __init__(self):
        self.base_url = settings.WEBSERVICE_BASE_URL

    def fetch_data(self, params=None):
        """Obtener datos desde webservice"""
        # Lógica desde XxxObtener.py
```

### 🔄 TRANSFORMER

```python
# processing/transformers/xxx_transformer.py
class XxxTransformer:
    """Transformaciones específicas para Xxx"""

    def transform_raw_to_schema(self, raw_data):
        """Convierte datos raw a schema Pydantic"""
```

### 🧪 TEST

```python
# test/test_xxx.py
def test_xxx_basic():
    """Test básico de funcionalidad Xxx"""
    from utils.adelantafactoring.v2.config.settings import settings
    assert settings.WEBSERVICE_BASE_URL == "https://webservice.adelantafactoring.com"

    # Test de datos simulados
    result = simulate_xxx_calculation()
    assert len(result) >= 0
```

## 🔧 **Imports Estándar**

```python
# ✅ SIEMPRE usar imports absolutos
from utils.adelantafactoring.v2.config.settings import settings
from utils.adelantafactoring.v2.schemas.xxx_schema import XxxSchema
from utils.adelantafactoring.v2.engines.calculation.xxx_calculation_engine import XxxCalculationEngine

# ✅ Fallback para desarrollo
try:
    from utils.adelantafactoring.v2.config.settings import settings
except ImportError:
    class _FallbackSettings:
        WEBSERVICE_BASE_URL = "https://webservice.adelantafactoring.com"
    settings = _FallbackSettings()
```

## 📊 **Checklist 5 Segundos**

-   [ ] ✅ Import absoluto desde `utils.adelantafactoring.v2.*`
-   [ ] ✅ Fallback implementado
-   [ ] ✅ Settings desde `config/settings.py`
-   [ ] ✅ Schema Pydantic con validación
-   [ ] ✅ Test básico funcionando
