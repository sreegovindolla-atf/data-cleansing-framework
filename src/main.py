import subprocess
import sys

def run_script(script_name: str):
    print(f"\n▶ Running {script_name}...")
    result = subprocess.run(
        [sys.executable, script_name],
        check=True
    )
    print(f"✔ Finished {script_name}")

if __name__ == "__main__":
    run_script("data_extraction.py")
    run_script("post_processing.py")

    print("\n🎉 Pipeline completed successfully")