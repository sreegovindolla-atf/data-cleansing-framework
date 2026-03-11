import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path


# =========================================================
# HELPERS
# =========================================================
def generate_run_id() -> str:
    """
    Format: <date_time>
    Example: 20260311_143205
    """
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def run_step(step_name: str, cmd: list[str]) -> None:
    """
    Run one pipeline step and stop the pipeline if it fails.
    """
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
    description="Run full extraction + post-processing pipeline."
)
parser.add_argument(
    "--run-id",
    default=None,
    help="Optional. If not passed, auto-generates in format YYYYMMDD_HHMMSS",
)
parser.add_argument(
    "--force-refresh",
    action="store_true",
    help="If set, pass --force-refresh to extraction steps.",
)

args = parser.parse_args()

RUN_ID = args.run_id.strip() if args.run_id else generate_run_id()
FORCE_REFRESH = args.force_refresh

print(f"[PIPELINE] RUN_ID = {RUN_ID}")
print(f"[PIPELINE] FORCE_REFRESH = {FORCE_REFRESH}")

PYTHON_EXE = sys.executable

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent

STEP_1A = BASE_DIR / "1a_data_extraction.py"
STEP_1B = BASE_DIR / "1b_post_processing.py"
STEP_1C = BASE_DIR / "1c_generic_extraction.py"
STEP_1D = BASE_DIR / "1d_generic_post_processing.py"

for step_file in [STEP_1A, STEP_1B, STEP_1C, STEP_1D]:
    if not step_file.exists():
        raise FileNotFoundError(f"Missing pipeline file: {step_file}")

UPSTREAM_IDS_FILE = PROJECT_ROOT / "data" / "outputs" / RUN_ID / f"{RUN_ID}_processed_indexes.txt"


def build_1c_cmd(entity: str) -> list[str]:
    cmd = [
        PYTHON_EXE,
        str(STEP_1C),
        "--entity",
        entity,
        "--run-id",
        RUN_ID,
        "--upstream-ids-file",
        str(UPSTREAM_IDS_FILE),
        "--use-main-source-query",
    ]
    if FORCE_REFRESH:
        cmd.append("--force-refresh")
    return cmd


def build_1d_cmd(entity: str) -> list[str]:
    return [
        PYTHON_EXE,
        str(STEP_1D),
        "--entity",
        entity,
        "--run-id",
        RUN_ID,
        "--upstream-ids-file",
        str(UPSTREAM_IDS_FILE),
    ]


# =========================================================
# PIPELINE ORDER
# 1.a
# 1.b
# 1.c project_type
# 1.d project_type
# 1.c asset
# 1.d asset
# 1.c beneficiary_group
# 1.d beneficiary_group
# =========================================================
try:
    # -------------------------
    # 1.a
    # -------------------------
    cmd_1a = [PYTHON_EXE, str(STEP_1A), "--run-id", RUN_ID]
    if FORCE_REFRESH:
        cmd_1a.append("--force-refresh")
    run_step("1a_data_extraction", cmd_1a)

    # -------------------------
    # 1.b
    # -------------------------
    cmd_1b = [PYTHON_EXE, str(STEP_1B), "--run-id", RUN_ID]
    run_step("1b_post_processing", cmd_1b)

    if not UPSTREAM_IDS_FILE.exists():
        raise FileNotFoundError(
            f"Missing upstream processed indexes file from previous step: {UPSTREAM_IDS_FILE}"
        )

    print(f"[PIPELINE] Upstream indexes file = {UPSTREAM_IDS_FILE}")

    # -------------------------
    # project_type
    # -------------------------
    run_step("1c_generic_extraction [project_type]", build_1c_cmd("project_type"))
    run_step("1d_generic_post_processing [project_type]", build_1d_cmd("project_type"))

    # -------------------------
    # asset
    # -------------------------
    run_step("1c_generic_extraction [asset]", build_1c_cmd("asset"))
    run_step("1d_generic_post_processing [asset]", build_1d_cmd("asset"))

    # -------------------------
    # beneficiary_group
    # -------------------------
    run_step("1c_generic_extraction [beneficiary_group]", build_1c_cmd("beneficiary_group"))
    run_step("1d_generic_post_processing [beneficiary_group]", build_1d_cmd("beneficiary_group"))

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