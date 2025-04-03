# dbt_eppo_sync/sync.py

import sys
import os
import json
import traceback
import datetime
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# Standard library imports for loading package data
import importlib.resources

# Import components from the package
try:
    from .parser import parse_dbt_artifacts, DbtParseError
    from .mapper import map_dbt_to_eppo_sync_payload, DbtMappingError
    from .eppo_client import EppoClient, EppoClientError, DEFAULT_EPPO_API_URL
    # Import the new validator function
    from .validator import validate_eppo_payload
except ImportError:
    # Allow running script directly for testing, assuming siblings exist
    print("Running sync.py directly, attempting sibling imports...")
    from parser import parse_dbt_artifacts, DbtParseError # type: ignore
    from mapper import map_dbt_to_eppo_sync_payload, DbtMappingError # type: ignore
    from eppo_client import EppoClient, EppoClientError, DEFAULT_EPPO_API_URL # type: ignore
    from validator import validate_eppo_payload # type: ignore

# Define the name of the schema file expected within the package
_SCHEMA_FILE_NAME = "eppo_metric_schema.json"

def _load_eppo_schema() -> Dict[str, Any]:
    """Loads the Eppo JSON schema from package data."""
    try:
        print(f"Loading Eppo schema '{_SCHEMA_FILE_NAME}'...")
        # Assumes the schema file is in the same directory as this sync.py
        # For Python 3.9+:
        schema_content = importlib.resources.files('dbt_eppo_sync').joinpath(_SCHEMA_FILE_NAME).read_text()
        # For Python 3.7, 3.8 (requires __init__.py in the directory):
        # with importlib.resources.path('dbt_eppo_sync', _SCHEMA_FILE_NAME) as schema_path:
        #     schema_content = schema_path.read_text()
        return json.loads(schema_content)
    except FileNotFoundError:
        raise RuntimeError(f"CRITICAL: Eppo schema file '{_SCHEMA_FILE_NAME}' not found within the package data. Ensure it's included.")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"CRITICAL: Failed to parse Eppo schema file '{_SCHEMA_FILE_NAME}'. Invalid JSON: {e}")
    except Exception as e:
        raise RuntimeError(f"CRITICAL: Unexpected error loading Eppo schema: {e}")

def run_sync(
    dbt_project_dir: str,
    manifest_path: str,
    eppo_api_key: str,
    eppo_base_url: Optional[str] = None,
    sync_tag: Optional[str] = None,
    dry_run: bool = False
) -> bool:
    """
    Orchestrates the synchronization process from dbt artifacts to Eppo
    using the bulk sync API.

    1. Initializes Eppo Client.
    2. Parses dbt YAML definitions and manifest.json.
    3. Maps dbt artifacts to the Eppo bulk sync payload structure.
    4. Validates the generated payload against the Eppo schema.
    5. Sends the generated payload to the Eppo API '/api/v1/metrics/sync'.

    Args:
        dbt_project_dir: Path to the dbt project directory (containing YAMLs).
        manifest_path: Path to the dbt manifest.json file (usually in 'target/').
        eppo_api_key: The API key for authenticating with Eppo.
        eppo_base_url: Optional override for the Eppo API base URL.
        sync_tag: Optional tag to identify this sync operation in Eppo.
                  Defaults to 'dbt-sync-<timestamp>'.
        dry_run: If True, performs parsing and mapping but does not call the Eppo API.
                 Prints the generated payload instead.

    Returns:
        True if the sync process completed successfully (or dry run was successful).
        False if a critical error stopped the process.
    """
    print("-" * 60)
    print("Starting dbt to Eppo synchronization (Bulk Sync Mode)...")
    print(f"dbt project dir: {dbt_project_dir}")
    print(f"dbt manifest path: {manifest_path}")
    print(f"Dry Run: {dry_run}")
    print("-" * 60)

    # Generate default sync tag if not provided
    effective_sync_tag = sync_tag or f"dbt-sync-{datetime.datetime.utcnow().isoformat()}"
    print(f"Using Sync Tag: {effective_sync_tag}")

    try:
        # --- 1. Initialize Eppo Client ---
        # Uses the updated client (v2) which has the sync_definitions method
        client = EppoClient(
            api_key=eppo_api_key,
            base_url=eppo_base_url or DEFAULT_EPPO_API_URL
        )
        print("Eppo client initialized.")

        # --- 2. Parse dbt Artifacts ---
        print(f"\nParsing dbt artifacts (YAMLs in '{dbt_project_dir}', manifest '{manifest_path}')...")
        # Uses the updated parser (v2) which reads manifest and links SQL
        all_metrics, all_semantic_models, sql_map = parse_dbt_artifacts(
            dbt_project_dir=dbt_project_dir,
            manifest_path=manifest_path
        )
        print(f"Parsed {len(all_metrics)} metrics and {len(all_semantic_models)} semantic models.")
        if not all_semantic_models: # Need at least semantic models to create fact sources
            print("Warning: No semantic models found. Cannot generate Eppo fact sources. Aborting sync.")
            # Or maybe allow syncing only metrics if Eppo supports it? Assuming SMs are needed.
            return True # Treat as success (nothing to sync)

        # --- 3. Map Artifacts to Eppo Payload ---
        print("\nMapping dbt artifacts to Eppo bulk sync payload...")
        # Uses the updated mapper (v2) which generates the bulk payload structure
        eppo_payload = map_dbt_to_eppo_sync_payload(
            dbt_metrics=all_metrics,
            dbt_semantic_models=all_semantic_models,
            sql_map=sql_map,
            sync_tag=effective_sync_tag
            # TODO: Add reference_url_base if needed/available
        )
        print(f"Generated payload with {len(eppo_payload.get('fact_sources',[]))} fact sources and {len(eppo_payload.get('metrics',[]))} metrics.")

        # --- 3.5 Validate Payload against Schema --- NEW STEP
        print("\nValidating generated payload against Eppo schema...")
        try:
            eppo_schema = _load_eppo_schema()
            validation_errors = validate_eppo_payload(eppo_payload, eppo_schema)
            if validation_errors:
                print("\nCRITICAL ERROR: Payload validation failed:", file=sys.stderr)
                for error_msg in validation_errors:
                    print(f"  - {error_msg}", file=sys.stderr)
                print("\nAborting sync due to validation errors.", file=sys.stderr)
                return False # Stop processing
            else:
                print("Payload validation successful.")
        except RuntimeError as schema_err:
            # Handle errors loading/parsing the schema itself
            print(schema_err, file=sys.stderr)
            return False # Stop processing if schema is broken

        # --- 4. Send Payload to Eppo (or Dry Run) ---
        if dry_run:
            print("\n--- DRY RUN: Eppo Sync Payload (Validated) ---")
            # Pretty print the generated payload for inspection
            import json
            print(json.dumps(eppo_payload, indent=2))
            print("--- END DRY RUN ---")
            print("\nDry run complete. No changes sent to Eppo.")
        else:
            print("\nSending bulk payload to Eppo API...")
            try:
                # Use the updated client's sync_definitions method
                sync_response = client.sync_definitions(payload=eppo_payload)
                print("Successfully sent payload to Eppo.")
                # Print response details if available (structure depends on Eppo API)
                if sync_response:
                    print("Eppo API Response:")
                    import json
                    print(json.dumps(sync_response, indent=2))
                else:
                    print("Eppo API returned an empty success response.")

            except EppoClientError as api_err:
                print(f"\nERROR: Failed to send payload to Eppo API: {api_err}", file=sys.stderr)
                # Include more details if helpful, e.g., validation errors from response
                if api_err.response_text:
                     print(f"--- Eppo Error Response Snippet --- \n{api_err.response_text[:1000]}\n---------------------------------", file=sys.stderr)
                return False # Treat API error during sync as failure

    # --- Handle Top-Level Errors ---
    except (DbtParseError, DbtMappingError) as e:
        print(f"\nCRITICAL ERROR during processing: {e}", file=sys.stderr)
        return False # Critical error, cannot proceed
    except EppoClientError as e:
         # Catch client errors during initialization
        print(f"\nCRITICAL ERROR: Eppo API client failed during initialization: {e}", file=sys.stderr)
        return False
    except ValueError as e: # e.g., missing API key
        print(f"\nCRITICAL ERROR: Configuration problem: {e}", file=sys.stderr)
        return False
    except FileNotFoundError as e:
         print(f"\nCRITICAL ERROR: Required file not found: {e}", file=sys.stderr)
         return False
    except Exception as e: # Catch any other unexpected major error
        print("\nCRITICAL UNEXPECTED ERROR during sync process:", file=sys.stderr)
        traceback.print_exc()
        return False

    # --- 5. Summary ---
    print("\n" + "=" * 60)
    print("Synchronization Summary:")
    if dry_run:
        print("  Mode: Dry Run (No changes sent to Eppo)")
    else:
        print("  Mode: Live Sync")
    print(f"  Status: {'Completed successfully'}") # Assuming success if we reach here without critical errors
    print(f"  Sync Tag Used: {effective_sync_tag}")
    print(f"  Fact Sources in Payload: {len(eppo_payload.get('fact_sources',[]))}")
    print(f"  Metrics in Payload: {len(eppo_payload.get('metrics',[]))}")
    print("=" * 60)

    return True # Return True indicating the process finished


# Example of how to run this from a wrapper script or CLI later
if __name__ == '__main__':
    print("Running dbt_eppo_sync.sync (v2 - Bulk Sync) as main script...")

    # Configuration - Replace with your actual values or load from env/config
    import os
    API_KEY = os.environ.get("EPPO_API_KEY")
    # Path to the root of your dbt project
    DBT_PROJECT_DIR = "./example_dbt_project" # Using the dummy dir created by parser.py example
    # Path to the manifest file (typically target/manifest.json)
    MANIFEST_PATH = "./example_dbt_project/target/manifest.json" # Using dummy manifest path
    # Set to True to test without making API calls
    DRY_RUN_ENABLED = True
    # Optional: Specify a custom sync tag
    CUSTOM_SYNC_TAG = os.environ.get("EPPO_SYNC_TAG") # Example: Read from env var

    # --- Pre-run Checks ---
    if not API_KEY:
         print("Error: EPPO_API_KEY environment variable not set.", file=sys.stderr)
         sys.exit(1)
    if not Path(DBT_PROJECT_DIR).is_dir():
         print(f"Error: dbt project path '{DBT_PROJECT_DIR}' not found or not a directory.", file=sys.stderr)
         sys.exit(1)
    if not Path(MANIFEST_PATH).is_file():
         print(f"Error: dbt manifest file '{MANIFEST_PATH}' not found.", file=sys.stderr)
         print("Hint: Run 'dbt parse' or 'dbt compile' in your project first, or run the parser.py example.", file=sys.stderr)
         # Optional: Try to run parser example to create dummy files if they don't exist
         # (This is fragile and only for basic testing)
         print("Attempting to run parser example to create dummy files...")
         try:
             import parser # Assuming parser.py is in the same directory
             # This relies on the parser's __main__ block creating the necessary files
             parser_main = getattr(parser, '__main__', None)
             if parser_main:
                 # Execute parser's main block - check its implementation
                 # This might require specific setup or might not work reliably
                 # Consider a dedicated setup script instead for robust testing
                 pass # Placeholder - executing other script's main is complex
             else:
                 print("Parser example block not found.")
             if not Path(MANIFEST_PATH).is_file(): # Check again
                 raise FileNotFoundError("Dummy manifest creation failed or skipped.")
             print("Dummy files potentially created by parser example.")
         except Exception as e:
             print(f"Could not ensure dummy files exist: {e}", file=sys.stderr)
             sys.exit(1)
         # End Optional Dummy File Creation

    # --- Execute the sync process ---
    print("\nExecuting run_sync...")
    sync_successful = run_sync(
        dbt_project_dir=DBT_PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=API_KEY,
        dry_run=DRY_RUN_ENABLED,
        sync_tag=CUSTOM_SYNC_TAG # Pass optional tag
        # eppo_base_url= # Optional override
    )

    if sync_successful:
        print("\nSync process finished.")
        sys.exit(0)
    else:
        print("\nSync process finished with errors.", file=sys.stderr)
        sys.exit(1)

