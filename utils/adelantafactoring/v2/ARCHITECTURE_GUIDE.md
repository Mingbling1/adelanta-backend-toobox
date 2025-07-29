# 🏗️ Adelanta Factoring V2 - Guía de Arquitectura Hexagonal

## 📋 **Principios de Colocación V2**

### 🎯 **¿Dónde va cada cosa?**

```
v2/
├── api/                    # 🌐 INTERFAZ PÚBLICA - Funciones simples de entrada
├── engines/                # ⚙️ MOTORES - Lógica de negocio reutilizable
├── io/                     # 📡 COMUNICACIÓN - Webservices, archivos, datos externos
├── processing/             # 🔄 PROCESAMIENTO - Transformaciones y validaciones
├── schemas/                # 📊 CONTRATOS - Validación Pydantic
└── config/                 # ⚙️ CONFIGURACIÓN - Settings centralizados
```

---

## 🔧 **Reglas de Refactorización**

### 📤 **1. DESDE V1 → V2: ¿Dónde colocar?**

| **Archivo V1**    | **Ubicación V2**              | **Razón**                                   |
| ----------------- | ----------------------------- | ------------------------------------------- |
| `*Calcular.py`    | `engines/calculation/`        | **Lógica de negocio matemática/financiera** |
| `*Obtener.py`     | `io/webservice/` o `io/data/` | **Comunicación externa (API/archivos)**     |
| `*Schema.py`      | `schemas/`                    | **Validación y contratos de datos**         |
| `*Transformer.py` | `processing/transformers/`    | **Transformaciones de datos**               |
| `*Validator.py`   | `processing/validators/`      | **Validaciones de lógica**                  |

### 🎯 **2. Funciones API: ¿Cómo exponer?**

```python
# ✅ EN api/*_api.py - Funciones públicas simples
def get_comisiones(fecha_corte: str = None) -> List[Dict]:
    """Interfaz pública simple para obtener comisiones"""
    # Orquesta engines + processing + io

def calculate_kpi(periodo: str) -> pd.DataFrame:
    """Interfaz pública para cálculos KPI"""
    # Usa engines/calculation + io/webservice
```

---

## 🏛️ **Patrones de Arquitectura**

### ⚙️ **3. ENGINES (Motores Reutilizables)**

```python
# engines/calculation/
class FinancialCalculationEngine:
    """Motor para cálculos financieros complejos"""
    def calculate_interest(self, principal, rate, time): pass
    def calculate_commissions(self, data): pass

# engines/validation/
class DataValidationEngine:
    """Motor para validaciones con Pydantic"""
    def validate_bulk(self, data, schema): pass
    def validate_financial_precision(self, amount): pass

# engines/data/
class DataExtractionEngine:
    """Motor para extracción optimizada"""
    def extract_webservice_data(self, url, params): pass
    def extract_csv_data(self, file_path): pass
```

### 🔄 **4. PROCESSING (Pipelines de Transformación)**

```python
# processing/transformers/
class ComisionesTransformer:
    """Transforma datos específicos de comisiones"""
    def transform_raw_to_schema(self, raw_data): pass

# processing/validators/
class ComisionesValidator:
    """Valida lógica específica de comisiones"""
    def validate_business_rules(self, data): pass

# processing/pipeline/
class ComisionesETLPipeline:
    """Orquesta todo el flujo de comisiones"""
    def run(self): pass  # usa engines + transformers + validators
```

### 📡 **5. IO (Comunicación Externa)**

```python
# io/webservice/
class ComisionesWebserviceClient:
    """Cliente específico para API de comisiones"""
    def fetch_kpi_data(self): pass

# io/data/
class ExcelDataSource:
    """Fuente de datos desde archivos Excel"""
    def read_excel_file(self, path): pass
```

---

## 🚀 **Flujo de Implementación**

### 📝 **PASOS PARA REFACTORIZAR UN MÓDULO**

1. **🔍 ANALIZAR V1**: Identificar `*Calcular.py`, `*Obtener.py`, `*Schema.py`

2. **📋 MAPEAR COMPONENTES**:

    ```
    XxxCalcular.py   → engines/calculation/xxx_calculation_engine.py
    XxxObtener.py    → io/webservice/xxx_client.py
    XxxSchema.py     → schemas/xxx_schema.py
    ```

3. **🔧 CREAR PROCESADORES**:

    ```
    processing/transformers/xxx_transformer.py
    processing/validators/xxx_validator.py
    ```

4. **🌐 EXPONER API**:

    ```
    api/xxx_api.py  # Función pública simple que orquesta todo
    ```

5. **🧪 CREAR TEST**:
    ```
    test/test_xxx.py  # Test simple de la funcionalidad completa
    ```

---

## ✅ **Checklist de Calidad**

### 🎯 **ANTES DE COMMIT**

-   [ ] **Imports absolutos**: `from utils.adelantafactoring.v2.config.settings import settings`
-   [ ] **Fallbacks implementados**: Para compatibilidad con desarrollo aislado
-   [ ] **Schema Pydantic**: Validación robusta de datos financieros
-   [ ] **Test funcionando**: Al menos 1 test básico por módulo
-   [ ] **Documentación**: Docstrings claros en funciones públicas
-   [ ] **Configuración centralizada**: URLs y constantes en `config/settings.py`

### 🔒 **PRINCIPIOS NO NEGOCIABLES**

-   ✅ **Preservar datos financieros originales** - NUNCA modificar source data
-   ✅ **Compatibilidad V1** - Mantener fallbacks durante transición
-   ✅ **Aislamiento V2** - Tests completamente independientes
-   ✅ **Configuración centralizada** - No hardcodear URLs/constantes

---

## 🎪 **Ejemplo Práctico: ComisionesCalcular**

```python
# ✅ ESTRUCTURA FINAL ESPERADA:
v2/
├── api/comisiones_api.py              # get_comisiones() - interfaz pública
├── engines/calculation/comisiones_calculation_engine.py  # Lógica matemática
├── io/webservice/comisiones_client.py # Comunicación con webservice
├── processing/
│   ├── transformers/comisiones_transformer.py  # Transformación datos
│   └── validators/comisiones_validator.py      # Validación lógica
├── schemas/comisiones_schema.py       # Pydantic schemas
└── test/test_comisiones.py           # Test de integración
```

---

## 🚀 **¿Listo para refactorizar?**

Con esta guía, cada módulo tendrá su lugar específico y mantendremos la arquitectura hexagonal limpia y escalable.

**¡Vamos por el siguiente módulo!** 🎯
