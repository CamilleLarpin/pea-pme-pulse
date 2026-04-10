import os
import subprocess
import tempfile
from pathlib import Path

from prefect import get_run_logger

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "dbt"
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "bootcamp-project-pea-pme")


def _run_dbt_cmd(
    subcommand: str, select: str | None = None, extra_args: list[str] | None = None
) -> subprocess.CompletedProcess:
    keyfile = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    method = "service-account" if keyfile else "oauth"
    extra = f"      keyfile: {keyfile}\n" if keyfile else ""

    profiles = f"""pea_pme_pulse:
  target: prod
  outputs:
    prod:
      type: bigquery
      method: {method}
      project: {GCP_PROJECT}
      dataset: silver
      threads: 4
      location: EU
{extra}"""

    cmd = [
        "dbt",
        subcommand,
        "--project-dir",
        str(DBT_PROJECT_DIR),
        "--target",
        os.environ.get("DBT_TARGET", "prod"),
    ]

    if select:
        cmd += ["--select", select]

    if extra_args:
        cmd += extra_args

    with tempfile.TemporaryDirectory() as profiles_dir:
        (Path(profiles_dir) / "profiles.yml").write_text(profiles)
        return subprocess.run(
            cmd + ["--profiles-dir", profiles_dir],
            capture_output=True,
            text=True,
        )


def _log_dbt_output(logger, result: subprocess.CompletedProcess) -> None:
    if result.stdout:
        logger.info(f"dbt stdout:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"dbt stderr:\n{result.stderr}")


def _dbt_deps() -> None:
    logger = get_run_logger()
    result = _run_dbt_cmd("deps")
    _log_dbt_output(logger, result)

    if result.returncode != 0:
        raise RuntimeError(f"dbt deps failed (code {result.returncode}).\n{result.stdout}")
    logger.info("dbt deps completed.")
