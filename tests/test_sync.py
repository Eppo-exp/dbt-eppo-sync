# tests/test_sync.py

import pytest
from unittest.mock import MagicMock, patch

# Assuming package structure allows this import
from dbt_eppo_sync import sync
from dbt_eppo_sync.parser import DbtParseError
from dbt_eppo_sync.mapper import DbtMappingError
from dbt_eppo_sync.eppo_client import EppoClientError

# --- Constants ---
API_KEY = "test-api-key"
PROJECT_DIR = "/fake/dbt/project"
MANIFEST_PATH = "/fake/dbt/project/target/manifest.json"

# --- Mocks (using unittest.mock via pytest fixture or direct patching) ---

@pytest.fixture(autouse=True) # Apply mocks to all tests in this module
def mock_dependencies(mocker):
    """Mock the core functions called by run_sync."""
    mocks = {
        'parse': mocker.patch('dbt_eppo_sync.sync.parse_dbt_artifacts', autospec=True),
        'map': mocker.patch('dbt_eppo_sync.sync.map_dbt_to_eppo_sync_payload', autospec=True),
        'client_init': mocker.patch('dbt_eppo_sync.sync.EppoClient', autospec=True),
    }
    # Mock the instance method sync_definitions
    mock_client_instance = mocks['client_init'].return_value
    mock_client_instance.sync_definitions = MagicMock(autospec=True)
    mocks['sync_defs'] = mock_client_instance.sync_definitions # Easier access

    # Default return values for mocks (can be overridden in tests)
    mocks['parse'].return_value = ([], [], {}) # metrics, semantic_models, sql_map
    mocks['map'].return_value = {"sync_tag": "mocked", "fact_sources": [], "metrics": []}
    mocks['sync_defs'].return_value = {"status": "mock success"}

    return mocks


# --- Test Cases ---

def test_run_sync_successful_live(mock_dependencies):
    """Test a successful run in live mode."""
    # Arrange: Setup mock return values if different from default
    mock_dependencies['parse'].return_value = (
        [{'name': 'm1'}], # metrics
        [{'name': 'sm1', '_model_unique_id': 'model.p.sm1'}], # semantic_models
        {'model.p.sm1': 'SELECT 1'} # sql_map
    )
    mock_dependencies['map'].return_value = {"sync_tag": "live", "fact_sources": [{}], "metrics": [{}]}
    mock_dependencies['sync_defs'].return_value = {"result": "ok"}

    # Act
    success = sync.run_sync(
        dbt_project_dir=PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=API_KEY,
        dry_run=False
    )

    # Assert
    assert success is True
    mock_dependencies['parse'].assert_called_once_with(dbt_project_dir=PROJECT_DIR, manifest_path=MANIFEST_PATH)
    mock_dependencies['map'].assert_called_once_with(
        dbt_metrics=[{'name': 'm1'}],
        dbt_semantic_models=[{'name': 'sm1', '_model_unique_id': 'model.p.sm1'}],
        sql_map={'model.p.sm1': 'SELECT 1'},
        sync_tag=pytest.approx(sync.datetime.datetime.utcnow().isoformat(), abs=1) # Check default tag format/time
    )
    mock_dependencies['client_init'].assert_called_once()
    mock_dependencies['sync_defs'].assert_called_once_with(payload={"sync_tag": "live", "fact_sources": [{}], "metrics": [{}]})

def test_run_sync_successful_dry_run(mock_dependencies):
    """Test a successful run in dry run mode."""
     # Arrange: Setup mock return values
    mock_dependencies['parse'].return_value = ([{'name': 'm1'}], [{'name': 'sm1'}], {})
    mock_dependencies['map'].return_value = {"sync_tag": "dry", "fact_sources": [{}], "metrics": [{}]}

    # Act
    success = sync.run_sync(
        dbt_project_dir=PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=API_KEY,
        dry_run=True # Enable dry run
    )

     # Assert
    assert success is True
    mock_dependencies['parse'].assert_called_once()
    mock_dependencies['map'].assert_called_once()
    mock_dependencies['client_init'].assert_called_once()
    # Crucially, assert sync_definitions was NOT called
    mock_dependencies['sync_defs'].assert_not_called()
    # TODO: Could use capsys fixture to check printed output for payload

def test_run_sync_parser_error(mock_dependencies):
    """Test failure if parser raises an error."""
    mock_dependencies['parse'].side_effect = DbtParseError("Failed to parse manifest")

    success = sync.run_sync(
        dbt_project_dir=PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=API_KEY,
        dry_run=False
    )

    assert success is False
    mock_dependencies['map'].assert_not_called()
    mock_dependencies['sync_defs'].assert_not_called()

def test_run_sync_mapper_error(mock_dependencies):
    """Test failure if mapper raises an error."""
    mock_dependencies['map'].side_effect = DbtMappingError("Failed to map metric")

    success = sync.run_sync(
        dbt_project_dir=PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=API_KEY,
        dry_run=False
    )

    assert success is False
    mock_dependencies['parse'].assert_called_once()
    mock_dependencies['sync_defs'].assert_not_called()

def test_run_sync_client_error(mock_dependencies):
    """Test failure if client raises an error during sync."""
    mock_dependencies['sync_defs'].side_effect = EppoClientError("API Forbidden", status_code=403)

    success = sync.run_sync(
        dbt_project_dir=PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=API_KEY,
        dry_run=False # Must be live mode to hit client error
    )

    assert success is False
    mock_dependencies['parse'].assert_called_once()
    mock_dependencies['map'].assert_called_once()
    mock_dependencies['sync_defs'].assert_called_once() # It was called, but failed

def test_run_sync_no_semantic_models(mock_dependencies):
    """Test behavior when parser finds no semantic models."""
    mock_dependencies['parse'].return_value = ([{'name': 'm1'}], [], {}) # No SMs

    success = sync.run_sync(
        dbt_project_dir=PROJECT_DIR,
        manifest_path=MANIFEST_PATH,
        eppo_api_key=API_KEY,
        dry_run=False
    )

    assert success is True # Should finish gracefully
    mock_dependencies['parse'].assert_called_once()
    mock_dependencies['map'].assert_not_called() # Mapping skipped
    mock_dependencies['sync_defs'].assert_not_called() # Sync skipped

# TODO: Add tests for:
# - Passing custom sync_tag
# - Passing custom eppo_base_url
# - FileNotFoundError handling for manifest/project dir
