# dbt_eppo_sync/cli.py

import click
import os
import sys
import pathlib
import yaml
from typing import Optional, Dict, Any

# Import the main sync function and constants/errors
try:
    from .sync import run_sync
    from .eppo_client import DEFAULT_EPPO_API_URL
    from .parser import DbtParseError
    from .mapper import DbtMappingError
    from .eppo_client import EppoClientError
except ImportError:
    # Handle case where script might be run directly during development
    # or if package structure isn't fully set up.
    print("Error: Could not perform relative imports. Ensure package structure is correct.", file=sys.stderr)
    # Attempt absolute imports if needed for specific dev setups (less ideal)
    # from dbt_eppo_sync.sync import run_sync
    # from dbt_eppo_sync.eppo_client import DEFAULT_EPPO_API_URL
    # ... etc ...
    sys.exit(1)


# Define the main command group/entry point
@click.command(
    help="Synchronizes dbt semantic layer definitions (semantic models, metrics) "
         "to Eppo using the bulk sync API."
)
@click.option(
    '--dbt-project-dir',
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True, path_type=pathlib.Path),
    help="Path to the root directory of your dbt project (containing dbt_project.yml)."
)
@click.option(
    '--manifest-path',
    required=True,
    type=click.Path(exists=True, file_okay=True, dir_okay=False, readable=True, path_type=pathlib.Path),
    help="Path to the dbt manifest.json artifact (usually in the 'target/' subdirectory)."
)
@click.option(
    '--eppo-api-key',
    envvar='EPPO_API_KEY', # Allow reading from environment variable
    help="Eppo API Key. Recommended to set via EPPO_API_KEY environment variable.",
    metavar='KEY'
    # Note: Avoid prompting directly for secrets in CLI for non-interactive use.
    # Rely on env var or direct option (less secure).
)
@click.option(
    '--eppo-base-url',
    default=DEFAULT_EPPO_API_URL,
    show_default=True,
    help="Base URL for the Eppo API.",
    metavar='URL'
)
@click.option(
    '--sync-tag',
    help="Optional tag to identify this sync operation in Eppo.",
    metavar='TAG'
)
@click.option(
    '--creator-email',
    help="Email of the user to set as metric creator (sync-level). Omit to clear.",
    metavar='EMAIL'
)
@click.option(
    '--updater-email',
    help="Email of the user to set as last updater (sync-level). Omit to clear.",
    metavar='EMAIL'
)
@click.option(
    '--team-name',
    help="Name of the team to associate with metrics (sync-level, 1–200 chars). Omit to clear.",
    metavar='NAME'
)
@click.option(
    '--dry-run',
    is_flag=True,
    default=False,
    help="Perform parsing and mapping but do not send data to Eppo API. Prints the payload instead."
)
@click.version_option(package_name='dbt-eppo-sync') # Reads version from pyproject.toml if installed
def _load_eppo_sync_config(dbt_project_dir: pathlib.Path) -> Dict[str, Any]:
    """Load optional eppo_sync config from dbt_project.yml (key 'eppo_sync' or 'vars.eppo_sync')."""
    config_path = dbt_project_dir / "dbt_project.yml"
    if not config_path.is_file():
        return {}
    try:
        with open(config_path, "r") as f:
            data = yaml.safe_load(f) or {}
    except (OSError, yaml.YAMLError):
        return {}
    # Prefer top-level eppo_sync, then vars.eppo_sync
    sync_config = data.get("eppo_sync") or data.get("vars", {}).get("eppo_sync")
    if not isinstance(sync_config, dict):
        return {}
    return {
        k: v for k, v in sync_config.items()
        if k in ("creator_email", "updater_email", "team_name") and v is not None
    }


def main(
    dbt_project_dir: pathlib.Path,
    manifest_path: pathlib.Path,
    eppo_api_key: Optional[str],
    eppo_base_url: str,
    sync_tag: Optional[str],
    creator_email: Optional[str],
    updater_email: Optional[str],
    team_name: Optional[str],
    dry_run: bool
):
    """
    Main entry point for the dbt-Eppo sync CLI.
    """
    # --- Input Validation ---
    if not eppo_api_key:
        click.echo("Error: Eppo API Key is required. Set the EPPO_API_KEY environment variable or use the --eppo-api-key option.", err=True)
        sys.exit(1)

    # Load optional sync config from dbt_project.yml; CLI options override
    file_config = _load_eppo_sync_config(dbt_project_dir)
    effective_creator = creator_email if creator_email is not None else file_config.get("creator_email")
    effective_updater = updater_email if updater_email is not None else file_config.get("updater_email")
    effective_team = team_name if team_name is not None else file_config.get("team_name")

    # Convert Path objects back to strings for run_sync function if needed
    project_dir_str = str(dbt_project_dir.resolve())
    manifest_path_str = str(manifest_path.resolve())

    click.echo("Starting dbt-Eppo Sync...")
    click.echo(f"  Project Directory: {project_dir_str}")
    click.echo(f"  Manifest Path: {manifest_path_str}")
    click.echo(f"  Eppo API URL: {eppo_base_url}")
    if sync_tag:
        click.echo(f"  Sync Tag: {sync_tag}")
    if effective_creator:
        click.echo(f"  Creator email: {effective_creator}")
    if effective_updater:
        click.echo(f"  Updater email: {effective_updater}")
    if effective_team:
        click.echo(f"  Team name: {effective_team}")
    click.echo(f"  Dry Run: {dry_run}")
    click.echo("-" * 20)

    try:
        # Call the core sync logic from sync.py
        success = run_sync(
            dbt_project_dir=project_dir_str,
            manifest_path=manifest_path_str,
            eppo_api_key=eppo_api_key,
            eppo_base_url=eppo_base_url,
            sync_tag=sync_tag,
            dry_run=dry_run,
            creator_email=effective_creator,
            updater_email=effective_updater,
            team_name=effective_team,
        )

        if success:
            click.echo(click.style("Sync process completed successfully.", fg='green'))
            sys.exit(0)
        else:
            # Errors during the run should have been printed by run_sync
            click.echo(click.style("Sync process finished with errors (see logs above).", fg='red'), err=True)
            sys.exit(1)

    # Catch specific errors from our modules for clearer CLI feedback
    except DbtParseError as e:
        click.echo(click.style(f"Error during dbt artifact parsing: {e}", fg='red'), err=True)
        sys.exit(1)
    except DbtMappingError as e:
        click.echo(click.style(f"Error during mapping to Eppo format: {e}", fg='red'), err=True)
        sys.exit(1)
    except EppoClientError as e:
        click.echo(click.style(f"Error communicating with Eppo API: {e}", fg='red'), err=True)
        sys.exit(1)
    except FileNotFoundError as e:
         click.echo(click.style(f"Error: Required file not found: {e}", fg='red'), err=True)
         sys.exit(1)
    except Exception as e:
        # Catch-all for unexpected errors
        click.echo(click.style(f"An unexpected error occurred: {e}", fg='red'), err=True)
        # Optionally print traceback for debugging unexpected errors
        # import traceback
        # traceback.print_exc()
        sys.exit(1)

# This allows running the CLI directly using `python -m dbt_eppo_sync.cli` during development
if __name__ == "__main__":
    main()
