FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Outils système utiles pour les dépendances Python courantes (lxml, cryptography, pandas, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    git \
    curl \
    ca-certificates \
    libxml2-dev \
    libxslt1-dev \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy dependency manifest first for layer caching
COPY pyproject.toml ./

# Copy project source
COPY . .

# Dependency installation
RUN pip install --upgrade pip \
    && pip install -e ".[dev]"

# Generic entry point — updated when DBT deployment is configured
CMD ["python", "-c", "print('Hello! Docker container is running successfully.')"]