import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


# =========================================================
# HELPERS
# =========================================================
def generate_run_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_step(step_name: str, cmd: list[str]) -> None:
    print("\n" + "=" * 80)
    print(f"[PIPELINE] Starting: {step_name}")
    print("[PIPELINE] Command:", " ".join(cmd))
    print("=" * 80)

    result = subprocess.run(cmd, check=False)

    if result.returncode != 0:
        raise RuntimeError(
            f"Step failed: {step_name} | returncode={result.returncode}"
        )

    print(f"[PIPELINE] Completed: {step_name}")


# =========================================================
# ARGS
# =========================================================
parser = argparse.ArgumentParser(
    description="Run 2a subsector extraction, 2b subsector post-processing, and 2c SDGs extraction."
)
parser.add_argument(
    "--run-id",
    default=None,
    help="Optional. If not passed, auto-generates in format YYYYMMDD_HHMMSS",
)
parser.add_argument(
    "--force-refresh",
    action="store_true",
    help="If set, pass --force-refresh to 2a subsector extraction.",
)

args = parser.parse_args()

RUN_ID = args.run_id.strip() if args.run_id else generate_run_id()
FORCE_REFRESH = args.force_refresh

print(f"[PIPELINE] RUN_ID = {RUN_ID}")
print(f"[PIPELINE] FORCE_REFRESH = {FORCE_REFRESH}")

PYTHON_EXE = sys.executable
BASE_DIR = Path(__file__).resolve().parent

STEP_2A = BASE_DIR / "2a_subsector_extraction.py"
STEP_2B = BASE_DIR / "2b_subsector_post_processing.py"
STEP_2C = BASE_DIR / "2c_sdgs_extraction.py"

for step_file in [STEP_2A, STEP_2B, STEP_2C]:
    if not step_file.exists():
        raise FileNotFoundError(f"Missing pipeline file: {step_file}")

RUN_OUTPUT_DIR = Path("data/outputs/project_attributes") / RUN_ID
UPSTREAM_IDS_FILE = RUN_OUTPUT_DIR / f"{RUN_ID}_processed_indexes.txt"


def build_2a_cmd() -> list[str]:
    cmd = [
        PYTHON_EXE,
        str(STEP_2A),
        "--run-id",
        RUN_ID,
    ]
    if FORCE_REFRESH:
        cmd.append("--force-refresh")
    return cmd


def build_2b_cmd() -> list[str]:
    return [
        PYTHON_EXE,
        str(STEP_2B),
        "--run-id",
        RUN_ID,
        "--upstream-ids-file",
        str(UPSTREAM_IDS_FILE),
    ]


def build_2c_cmd() -> list[str]:
    return [
        PYTHON_EXE,
        str(STEP_2C),
        "--run-id",
        RUN_ID,
        "--upstream-ids-file",
        str(UPSTREAM_IDS_FILE),
    ]


# =========================================================
# PIPELINE
# 2a -> 2b -> 2c
# =========================================================
try:
    # -------------------------
    # 2a
    # -------------------------
    run_step("2a_subsector_extraction", build_2a_cmd())

    # after 2a, processed_indexes for this run must exist
    if not UPSTREAM_IDS_FILE.exists():
        raise FileNotFoundError(
            f"Missing processed indexes file created by 2a: {UPSTREAM_IDS_FILE}"
        )

    print(f"[PIPELINE] Processed indexes file from 2a = {UPSTREAM_IDS_FILE}")

    # -------------------------
    # 2b
    # -------------------------
    run_step("2b_subsector_post_processing", build_2b_cmd())

    # -------------------------
    # 2c
    # -------------------------
    run_step("2c_sdgs_extraction", build_2c_cmd())

    print("\n" + "=" * 80)
    print("[PIPELINE] ALL STEPS COMPLETED SUCCESSFULLY")
    print(f"[PIPELINE] Final RUN_ID: {RUN_ID}")
    print("=" * 80)

except Exception as e:
    print("\n" + "=" * 80)
    print("[PIPELINE] FAILED")
    print(f"[PIPELINE] RUN_ID: {RUN_ID}")
    print(f"[PIPELINE] ERROR: {e}")
    print("=" * 80)
    sys.exit(1)