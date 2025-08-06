# Adelanta Backend Toolbox - AI Coding Instructions

## Architecture Overview

This is a **Financial ETL Processing System** for Adelanta Factoring built with FastAPI, specializing in financial data extraction, transformation, and validation using webservices.

### Core Tech Stack

-   **FastAPI 0.104+** (Python 3.12+) with async/await patterns
-   **Pydantic 2.0+** with RUST validation for financial data integrity
-   **Pandas** for ETL transformations (migrating to Polars)
-   **MySQL 8.0+** (Production) / SQLite (Testing)
-   **Alembic** for database migrations

## 🚨 CRITICAL V2 MIGRATION RULES - NON-NEGOTIABLE

### ⚡ PRIORITY #1: COMPATIBILITY OVER EVERYTHING

```python
# ✅ ALWAYS: Copy V1 logic EXACTLY
# ❌ NEVER: Modify, improve, or "optimize" V1 business logic
# ❌ NEVER: Change calculation results
# ❌ NEVER: Invent new validation rules
```

### 🔒 V2 Migration Principles

1. **COPY, DON'T CREATE**: V2 is architectural restructuring ONLY
2. **PRESERVE EXACT LOGIC**: Every calculation must produce identical results
3. **SAME CONSTRUCTOR SIGNATURE**: Constructor parameters must match V1 exactly
4. **SAME METHOD NAMES**: Public methods must have identical names and signatures
5. **SIMPLE TESTS**: One basic test per module, reuse test patterns

### 📋 V2 Refactoring Checklist

Before ANY V2 work:

-   [ ] Read V1 source code completely
-   [ ] Identify exact constructor signature
-   [ ] List all public methods and their signatures
-   [ ] Copy business logic line-by-line
-   [ ] Verify identical results with simple test

### 🎯 V2 Architecture Mapping

```bash
# V1 → V2 EXACT MAPPING:
XxxCalcular.py    → engines/xxx_calculation_engine.py (copy logic exactly)
XxxObtener.py     → io/webservice/xxx_client.py (copy logic exactly)
XxxSchema.py      → schemas/xxx_schema.py (copy + add ConfigDict)
                  → api/xxx_api.py (wrapper class with same interface)
                  → test/test_xxx.py (simple compatibility test)
```

### 🧪 Testing Strategy - SIMPLIFIED

```python
# ✅ SINGLE TEST PATTERN FOR ALL MODULES:
def test_xxx_v2_basic():
    """Test that V2 has same interface as V1"""
    # 1. Create V2 instance with same parameters as V1
    # 2. Verify constructor works
    # 3. Verify main methods exist and are callable
    # 4. Done - NO complex business logic testing
```

### ⚠️ What NOT to do in V2

-   ❌ Don't add "improvements" to business logic
-   ❌ Don't change validation rules
-   ❌ Don't modify date handling unless V1 is broken
-   ❌ Don't create complex tests
-   ❌ Don't optimize performance (yet)
-   ❌ Don't add new features

## Financial Data Integrity

```python
# NEVER modify original financial data - always preserve source values
@field_validator("MontoDevolucion", mode="before")
@classmethod
def validate_required_float_fields(cls, v, info):
    # Convert but preserve original financial precision
```

## ETL Pipeline Structure

The system follows a **Calculator → Obtainer → Schema** pattern:

-   `*Calcular.py` - Business logic and transformations
-   `*Obtener.py` - Data extraction from webservices
-   `*Schema.py` - Pydantic validation with financial field validators

## V2 Schema Requirements

```python
# ALL V2 Schemas MUST include:
model_config = ConfigDict(arbitrary_types_allowed=True)
```

## Testing Commands

```bash
# Development tests
pytest

# Production tests
pytest -m production

# V2 specific tests
cd utils/adelantafactoring/v2 && python -m pytest test/ -v
```

## Critical Integration Points

### External Webservice Communication

-   Base URL: `https://webservice.adelantafactoring.com`
-   Implements retry logic with `tenacity` decorators
-   Handles both sync/async patterns in `BaseObtener`

### Financial Validation Engine

-   All schemas inherit robust field validators for currency conversion
-   Handles `fecha_corte` parameters for temporal financial reporting
-   Implements fuzzy matching for entity reconciliation

## Repository Pattern

```python
class KPIRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(KPIModel, db)
```

## Date Handling

```python
# Always normalize financial dates to midnight UTC
df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
df[col] = df[col].dt.tz_localize(None).dt.normalize()
```

## Error Handling

-   Use `config.logger` for financial audit trails
-   Preserve data integrity even on validation failures
-   Implement `@BaseCalcular.timeit` for performance monitoring

## V2 SUCCESS CRITERIA

✅ **DONE CORRECTLY**: When V2 produces identical results to V1 with minimal effort
❌ **FAILED**: When V2 takes more than 1 hour per module or changes business logic
