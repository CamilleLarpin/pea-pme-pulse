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

# Copie d'abord les fichiers de dépendances pour tirer parti du cache Docker
COPY pyproject.toml setup.py setup.cfg requirements.txt ./

# Puis le reste du dépôt
COPY . .

RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install prefect \
    && if [ -f requirements.txt ]; then pip install -r requirements.txt; fi \
    && if [ -f pyproject.toml ] || [ -f setup.py ] || [ -f setup.cfg ]; then pip install -e ".[dev]"; fi

# Répertoire de travail par défaut. Ajustez la commande au script d'entrée de votre projet.
CMD ["python", "-m", "pea_pme_pulse"]