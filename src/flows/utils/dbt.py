import os
import subprocess
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).parent.parent.parent.parent / "dbt"


def _run_dbt(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True)


def _log_dbt_output(logger, result: subprocess.CompletedProcess) -> None:
    if result.stdout:
        logger.info(f"dbt stdout:\n{result.stdout}")
    if result.stderr:
        logger.warning(f"dbt stderr:\n{result.stderr}")


def _base_dbt_cmd(subcommand: str, select: str) -> list[str]:
    cmd = [
        "dbt", subcommand,
        "--project-dir", str(DBT_PROJECT_DIR),
        "--target", os.environ.get("DBT_TARGET", "prod"),
    ]
    if select:
        cmd += ["--select", select]
    return cmd

def _dbt_deps() -> None:
    result = _run_dbt(_base_dbt_cmd("deps", ""))
    # deps n'a pas de --select donc on construit la commande manuellement
    cmd = [
        "dbt", "deps",
        "--project-dir", str(DBT_PROJECT_DIR),
        "--target", os.environ.get("DBT_TARGET", "prod"),
    ]
    result = _run_dbt(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"dbt deps failed (code {result.returncode}).\n{result.stdout}")