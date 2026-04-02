import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from bronze.amf_ingest import run_pipeline

if __name__ == "__main__":
    run_pipeline()
