FROM python:3.12-slim-bookworm

# 🚀 MÉTODO OFICIAL: Copiar uv desde imagen oficial (más rápido y confiable)
COPY --from=ghcr.io/astral-sh/uv:0.8.5 /uv /uvx /bin/

# 🔧 Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    locales-all \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 🔒 GESTIÓN MODERNA: Copiar archivos de configuración de uv
COPY pyproject.toml uv.lock ./

# 🏎️ Usar uv sync para instalación determinística y ultra-rápida
RUN uv sync --locked --no-dev

COPY . .

EXPOSE 8000

# 🚀 MÉTODO OFICIAL UV: Usar uv run para ejecutar la aplicación
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000","--use-colors", "--log-config=log_conf.yaml"]