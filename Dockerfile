FROM python:3.12-slim

# 🚀 Instalar uv para instalación ultra-rápida de paquetes
RUN apt-get update && apt-get install -y \
    locales-all \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh

# Añadir uv al PATH
ENV PATH="/root/.cargo/bin:$PATH"

WORKDIR /app

# 🏎️ Usar uv en lugar de pip para instalación 10x más rápida
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

COPY . .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2", "--use-colors", "--log-config=log_conf.yaml"]
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "info", "--reload"]