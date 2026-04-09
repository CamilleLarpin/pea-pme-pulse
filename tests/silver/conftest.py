import os
from pathlib import Path

import pytest
from dotenv import load_dotenv
from loguru import logger


@pytest.fixture(scope="session")
def env():
    """
    Pytest fixture that provides environment configuration.
    It loads variables from the .env file at the project root.
    """
    # 1. Locate project root (assuming conftest.py is inside /tests/)
    # resolve().parents[1] goes up one level from /tests/ to /project_root/
    base_dir = Path(__file__).resolve().parents[2]
    env_path = base_dir / ".env"

    # 2. Load the .env file into os.environ
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
        logger.info(f"✅ Test environment variables loaded from: {env_path}")
    else:
        logger.warning(
            f"⚠️ .env file not found at {env_path}. Relying on system environment variables."
        )

    # 3. Construct the config dictionary
    config = {
        "BASE_DIR": base_dir,
        "GCP_PROJECT_ID": os.getenv("GCP_PROJECT_ID"),
        "BQ_DATASET_BRONZE": os.getenv("BQ_DATASET_BRONZE"),
        "BQ_DATASET_SILVER": os.getenv("BQ_DATASET_SILVER"),
        "GROQ_API_KEY": os.getenv("GROQ_API_KEY"),
        "GEMINI_API_KEY": os.getenv("GEMINI_API_KEY"),
        "GOOGLE_APPLICATION_CREDENTIALS": os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
    }

    # 4. Safety Check: Alert if critical keys are missing
    missing_keys = [k for k, v in config.items() if v is None]
    if missing_keys:
        logger.error(
            f"❌ Missing critical environment variables for tests: {', '.join(missing_keys)}"
        )

    return config
