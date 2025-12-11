# run_pipeline.py
import sys
import os
import subprocess

# -----------------------------
# Ensure src/ is in Python path
# -----------------------------
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from utils.logger import logger

# List of scripts inside src/
SCRIPTS = [
    "clean_data.py",
    "fetch_data.py",
    "main_features.py",
    "model.py"
]

def run_script(script_name):
    script_path = os.path.join(SRC_DIR, script_name)
    print(f"\n=== Running {script_name} ===")
    result = subprocess.run([sys.executable, script_path], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        logger.error(f"{script_name} failed! Exiting pipeline.")
        sys.exit(1)
    print(f"=== {script_name} completed successfully ===\n")

def main():
    print("Starting FULL DEMAND FORECASTING PIPELINE")

    for script in SCRIPTS:
        run_script(script)

    print("FULL PIPELINE EXECUTED SUCCESSFULLY!")

if __name__ == "__main__":
    main()
