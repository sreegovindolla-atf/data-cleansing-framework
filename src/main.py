import subprocess
import sys
from datetime import datetime

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

print(f"Starting pipeline with run_id={run_id}")

subprocess.run(
    [sys.executable, "src/data_extraction.py", "--run-id", run_id],
    check=True
)

subprocess.run(
    [sys.executable, "src/post_processing.py", "--run-id", run_id],
    check=True
)

print("Pipeline completed successfully")