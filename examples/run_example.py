# examples/run_example.py
# Example script to demonstrate running the sync process in dry-run mode.

import os
import sys
from pathlib import Path

# Adjust the path to import from the parent directory's 'dbt_eppo_sync' module
# This assumes run_example.py is inside the 'examples' folder
script_dir = Path(__file__).parent
project_root = script_dir.parent
sys.path.insert(0, str(project_root))

try:
    # Import the main sync function
    from dbt_eppo_sync.sync import run_sync
except ImportError as e:
    print(f"Error: Could not import 'run_sync'. Make sure '{project_root / 'dbt_eppo_sync'}' exists and is a package.")
    print(f"Import error: {e}")
    sys.exit(1)

# --- Configuration ---

# 1. Eppo API Key (Placeholder - DO NOT COMMIT REAL KEYS)
#    For a real run, set this environment variable securely.
EPPO_API_KEY = os.environ.get("EPPO_API_KEY", "YOUR_EPPO_API_KEY_PLACEHOLDER")
if EPPO_API_KEY == "YOUR_EPPO_API_KEY_PLACEHOLDER":
    print("Warning: Using placeholder Eppo API key. Set the EPPO_API_KEY environment variable for a real run.")

# 2. Paths to dbt artifacts (relative to this script's location)
#    Assumes dbt project files are within the 'examples' directory for this demo
DBT_PROJECT_DIR = str(script_dir) # Directory containing the sample YAMLs
MANIFEST_PATH = str(script_dir / "sample_manifest.json") # Path to the sample manifest

# 3. Dry Run Setting
#    Set to False to attempt a real sync (requires valid API key and Eppo setup)
DRY_RUN = True

# --- Execute Sync ---

print("--- Running dbt-Eppo Sync Example (Dry Run) ---")
print(f"Project Dir: {DBT_PROJECT_DIR}")
print(f"Manifest Path: {MANIFEST_PATH}")
print(f"Dry Run: {DRY_RUN}")
print("-" * 40)

try:
    # Call the main sync function
    success = run_sync(
        dbt_project_dir=DBT_PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=EPPO_API_KEY,
        dry_run=DRY_RUN
        # sync_tag="example-run", # Optional tag
        # eppo_base_url= # Optional override
    )

    if success:
        print("\n--- Example Sync Process Finished ---")
        if DRY_RUN:
            print("Result: Dry run completed. Review the payload output above.")
        else:
            print("Result: Sync attempted. Check Eppo UI and logs for details.")
    else:
        print("\n--- Example Sync Process Finished with Errors ---")

except FileNotFoundError as e:
    print(f"\nError: File not found. Ensure sample files exist in the '{script_dir}' directory.")
    print(e)
except Exception as e:
    print(f"\nAn unexpected error occurred during the example run:")
    import traceback
    traceback.print_exc()

