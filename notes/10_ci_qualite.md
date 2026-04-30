# 10 — CI & Qualité du code

## GitHub Actions — `ci.yml`

**Déclencheurs :**
- Toute Pull Request
- Push sur `main` ou `develop`

**Runner :** `ubuntu-latest`

**Python :** 3.11  
**Poetry :** 2.3.2 (installé via `pipx`)

### Étapes dans l'ordre

1. `actions/checkout@v6` — Checkout du repo
2. `actions/setup-python@v6` avec `python-version: "3.11"`
3. `pipx install poetry==2.3.2` + ajout au PATH
4. `poetry check --lock` — Vérifie la cohérence de `poetry.lock` avec `pyproject.toml`
5. `poetry install --extras dev --no-root` — Installe dépendances + groupe dev (ruff, pytest)
6. `poetry run ruff check .` — Lint
7. `poetry run ruff format --check .` — Vérification formatage (ne modifie pas)
8. `poetry run pytest -q` — Tests unitaires

**Note importante :** La CI ne lance pas `dbt test` — les modèles SQL ne sont pas testés en CI.

---

## Ruff — Linter & Formateur

**Config dans `pyproject.toml` :**

```toml
[tool.ruff]
line-length = 100
target-version = "py311"
exclude = [".git", "__pycache__", ".venv", "build", "dist"]

[tool.ruff.lint]
select = [
    "E",   # pycodestyle — style PEP8
    "F",   # pyflakes — bugs (unused imports, variables)
    "I",   # isort — ordre des imports
    "UP",  # pyupgrade — modernisation Python
    "B",   # flake8-bugbear — patterns problématiques
    "SIM", # flake8-simplify — simplifications
]
ignore = ["E501"]  # Longueur de ligne (gérée par le formateur, pas le linter)

[tool.ruff.format]
quote-style = "double"
indent-style = "space"
line-ending = "auto"
```

**Commandes :**
```bash
make lint       # ruff check .
make lint-fix   # ruff check . --fix
make format     # ruff format .
```

---

## pytest — Tests unitaires

**Config dans `pyproject.toml` :**
```toml
[tool.pytest.ini_options]
pythonpath = ["src"]
norecursedirs = [".git", ".venv", "dist", "build", "dbt/dbt_packages"]
```

**Structure des tests :**

```
tests/
  __init__.py
  bronze/
    test_bronze_amf_flux.py
    test_rss_abcbourse.py
    test_rss_google_news.py
    test_rss_yahoo_fr.py
    test_yahoo_ohlcv_bronze.py
    test_amf_ingest.py
  silver/
    __init__.py
    conftest.py
    test_compute_silver.py
    test_insider_parser.py
  gold/
    __init__.py
    test_insider_flow.py
    test_stocks_score.py   ← Tests de la logique de scoring (miroir Python de stocks_score.sql)
```

**Couverture :**
- Bronze : 6 fichiers de test (ingestion, fetch, match)
- Silver : 2 fichiers + conftest (indicateurs techniques, parsing PDF)
- Gold : 2 fichiers (scoring insider, scoring technique)

**Commande :**
```bash
make test    # pytest -q
```

---

## Makefile — Cibles disponibles

```bash
make install     # poetry install --no-root
make lint        # ruff check .
make lint-fix    # ruff check . --fix
make lock        # poetry lock
make format      # ruff format .
make test        # pytest -q
make quality     # lock + install + lint-fix + format + test (tout en séquence)
make gcp-auth    # gcloud auth login + ADC
make gcp-setup   # gcp-auth + gcloud config set project
make gcp-check   # liste comptes + projet actif
make gcp-reset   # revoke ADC + unset project
```

---

## PR Template

Fichier : `.github/pull_request_template.md`

Sections :
1. **Résumé** — Ce que la PR fait et pourquoi
2. **Type de changement** — Bug fix / Nouvelle feature / Infrastructure CI / Refactoring
3. **Test plan** — CI passe (ruff + pytest) + testé localement

---

## pyproject.toml — Structure

Le fichier contient deux sections de dépendances (legacy) :

- **`[project]`** — Section canonique PEP 621, utilisée par Poetry 2.x et pip
- **`[tool.poetry.dependencies]`** — Section legacy Poetry 1.x, contient seulement `python = "^3.11"` et `pdfplumber = "^0.11.9"` (valeurs qui remplacent celles de `[project]`)

La CI utilise Poetry comme outil canonique. En cas de conflit, `[project]` est la source de vérité.

---

## Dépendances dev

```toml
[project.optional-dependencies]
dev = [
    "ruff",
    "pytest",
    "pytest-mock (>=3.15.1,<4.0.0)",
]
```

Installées avec `poetry install --extras dev`.
