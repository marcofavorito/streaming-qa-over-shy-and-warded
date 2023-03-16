from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.absolute()
THIRDPARTY_DIR = ROOT_DIR / "third_party"
ORIGINAL_DATASETS_DIR = THIRDPARTY_DIR / "original_datasets"
ORIGINAL_PROGRAMS_DIR = THIRDPARTY_DIR / "original_programs"
DATASETS_DIR = ROOT_DIR / "datasets"
CHASEBENCH = THIRDPARTY_DIR / "chasebench"
CHASEBENCH_SCENARIOS = CHASEBENCH / "scenarios"
