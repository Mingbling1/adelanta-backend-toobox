FROM python:3.13-slim-bookworm

# 🚀 Instalar uv para gestión moderna de dependencias
RUN apt-get update && apt-get install -y \
    locales-all \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Instalar uv en paso separado
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

WORKDIR /app

# 🔒 GESTIÓN MODERNA: Copiar archivos de configuración de uv
COPY pyproject.toml uv.lock ./

# 🏎️ Usar uv sync para instalación determinística y ultra-rápida
RUN /root/.cargo/bin/uv sync --locked --no-dev

COPY . .

EXPOSE 8000

# 🚀 UVICORN OPTIMIZADO PARA 2GB RAM / 2 CORES
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--use-colors", "--log-config=log_conf.yaml"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--reload"]