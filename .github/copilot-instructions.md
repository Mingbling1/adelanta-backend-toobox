# Adelanta Backend Toolbox - AI Coding Instructions

## Architecture Overview

This is a **Financial ETL Processing System** for Adelanta Factoring built with FastAPI, specializing in financial data extraction, transformation, and validation using webservices.

### Core Tech Stack

-   **FastAPI 0.104+** (Python 3.12+) with async/await patterns
-   **Pydantic 2.0+** with RUST validation for financial data integrity
-   **Pandas** for ETL transformations (migrating to Polars)
-   **MySQL 8.0+** (Production) / SQLite (Testing)
-   **Alembic** for database migrations

## Critical Patterns

### Financial Data Integrity

```python
# NEVER modify original financial data - always preserve source values
@field_validator("MontoDevolucion", mode="before")
@classmethod
def validate_required_float_fields(cls, v, info):
    # Convert but preserve original financial precision
```

### ETL Pipeline Structure

The system follows a **Calculator → Obtainer → Schema** pattern:

-   `*Calcular.py` - Business logic and transformations
-   `*Obtener.py` - Data extraction from webservices
-   `*Schema.py` - Pydantic validation with financial field validators

### Performance Optimizations

```python
# Use list comprehensions with Pydantic for bulk validation
schema_fields = set(CXCAcumuladoDIMCalcularSchema.model_fields.keys())
df_filtered = df[df.columns.intersection(schema_fields)]
return [Schema(**registro).model_dump() for registro in df_filtered.to_dict(orient="records")]
```

## Workflow Commands

### Testing Strategy

```bash
# Development tests (SQLite auto-reset)
pytest

# Production tests (MySQL real data)
pytest -m production

# Specific financial module tests
pytest -k "cxc" -v
```

### Financial ETL Processing

Key modules in `utils/adelantafactoring/`:

-   `CXCETLProcessor` - Complete CXC pipeline (3-table ETL)
-   `KPICalcular` - Financial metrics with fecha_corte handling
-   `*AutodetraccionesCalcular` - Tax calculation modules

## Project-Specific Conventions

### Date Handling

```python
# Always normalize financial dates to midnight UTC
df[col] = pd.to_datetime(df[col], format=fmt, errors="coerce")
df[col] = df[col].dt.tz_localize(None).dt.normalize()
```

### Repository Pattern

```python
class KPIRepository(BaseRepository):
    def __init__(self, db: DB) -> None:
        super().__init__(KPIModel, db)
```

### Error Handling

-   Use `config.logger` for financial audit trails
-   Preserve data integrity even on validation failures
-   Implement `@BaseCalcular.timeit` for performance monitoring

## Critical Integration Points

### External Webservice Communication

-   Base URL: `https://webservice.adelantafactoring.com`
-   Implements retry logic with `tenacity` decorators
-   Handles both sync/async patterns in `BaseObtener`

### Financial Validation Engine

-   All schemas inherit robust field validators for currency conversion
-   Handles `fecha_corte` parameters for temporal financial reporting
-   Implements fuzzy matching for entity reconciliation

## Migration Notes (v2 Architecture)

When refactoring to `utils/adelantafactoring/v2/`:

1. Maintain **backward compatibility** with existing imports
2. Preserve **financial data integrity** validation patterns
3. Follow **hexagonal architecture**: api/ → engines/ → io/ → processing/
4. Keep existing Calculator/Obtainer/Schema dependency patterns
