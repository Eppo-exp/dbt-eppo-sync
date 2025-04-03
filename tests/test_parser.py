# tests/test_parser.py

import pytest
import json
import yaml
from pathlib import Path

# Assuming your package structure allows this import
from dbt_eppo_sync.parser import parse_dbt_artifacts, DbtParseError

# --- Fixtures (Example using pytest's tmp_path) ---

@pytest.fixture
def sample_project_dir(tmp_path: Path) -> Path:
    """Creates a temporary directory structure for a sample dbt project."""
    proj_dir = tmp_path / "sample_dbt_project"
    models_dir = proj_dir / "models"
    target_dir = proj_dir / "target"
    models_dir.mkdir(parents=True)
    target_dir.mkdir(parents=True)

    # Create dummy dbt_project.yml
    (proj_dir / "dbt_project.yml").write_text("name: 'test_project'")

    # Create dummy manifest.json
    manifest_content = {
        "metadata": {"project_name": "test_project"},
        "nodes": {
            "model.test_project.my_model": {
                "unique_id": "model.test_project.my_model",
                "resource_type": "model", "name": "my_model", "package_name": "test_project",
                "compiled_code": "SELECT 1 as id, 'A' as value, current_timestamp() as event_ts"
            }
        },
        "sources": {}, "metrics": {}, "semantic_models": {}, "exposures": {},
        "macros": [], "docs": {}, "disabled": [], "child_map": {}, "parent_map": {}
    }
    (target_dir / "manifest.json").write_text(json.dumps(manifest_content))

    # Create dummy semantic model yaml
    sm_yaml_content = """
semantic_models:
  - name: my_semantic_model
    model: ref('my_model')
    entities:
      - name: primary_entity
        type: primary
        expr: id
    dimensions:
      - name: value_dim
        type: categorical
        expr: value
      - name: event_time
        type: time # Important for timestamp detection
        expr: event_ts
    measures:
      - name: model_count
        agg: count
        expr: id
"""
    (models_dir / "schema.yml").write_text(sm_yaml_content)

    # Create dummy metrics yaml
    metrics_yaml_content = """
metrics:
  - name: total_count
    label: "Total Count"
    type: count
    measure:
      name: model_count
"""
    (models_dir / "metrics.yml").write_text(metrics_yaml_content)

    return proj_dir

# --- Test Cases ---

def test_parse_successful(sample_project_dir: Path):
    """Test successful parsing of valid artifacts."""
    manifest_path = sample_project_dir / "target" / "manifest.json"
    metrics, semantic_models, sql_map = parse_dbt_artifacts(
        dbt_project_dir=str(sample_project_dir),
        manifest_path=str(manifest_path)
    )

    assert len(metrics) == 1
    assert metrics[0]['name'] == 'total_count'

    assert len(semantic_models) == 1
    assert semantic_models[0]['name'] == 'my_semantic_model'
    # Check if model linking worked
    assert '_model_unique_id' in semantic_models[0]
    assert semantic_models[0]['_model_unique_id'] == 'model.test_project.my_model'

    assert len(sql_map) == 1
    assert 'model.test_project.my_model' in sql_map
    assert "SELECT 1 as id" in sql_map['model.test_project.my_model']

def test_parse_missing_manifest(sample_project_dir: Path):
    """Test error handling when manifest.json is missing."""
    manifest_path = sample_project_dir / "target" / "non_existent_manifest.json"
    with pytest.raises(DbtParseError, match="Manifest file not found"):
        parse_dbt_artifacts(
            dbt_project_dir=str(sample_project_dir),
            manifest_path=str(manifest_path)
        )

def test_parse_invalid_yaml(sample_project_dir: Path):
    """Test error handling with invalid YAML content."""
    manifest_path = sample_project_dir / "target" / "manifest.json"
    # Overwrite a yaml file with invalid content
    (sample_project_dir / "models" / "schema.yml").write_text("invalid: yaml: here")

    with pytest.raises(DbtParseError, match="Error parsing YAML file"):
        parse_dbt_artifacts(
            dbt_project_dir=str(sample_project_dir),
            manifest_path=str(manifest_path)
        )

def test_parse_missing_sql(sample_project_dir: Path):
    """Test scenario where linked model exists but has no compiled SQL."""
    manifest_path = sample_project_dir / "target" / "manifest.json"
    # Modify manifest to remove compiled_code
    with open(manifest_path, 'r+') as f:
        manifest_data = json.load(f)
        del manifest_data['nodes']['model.test_project.my_model']['compiled_code']
        f.seek(0)
        json.dump(manifest_data, f)
        f.truncate()

    # Parsing should succeed but sql_map will be empty or lack the entry
    # The parser currently prints a warning in this case.
    # Depending on desired behavior, could assert specific warnings or map content.
    _metrics, _semantic_models, sql_map = parse_dbt_artifacts(
        dbt_project_dir=str(sample_project_dir),
        manifest_path=str(manifest_path)
    )
    assert 'model.test_project.my_model' not in sql_map
    # TODO: Capture and assert warnings if using logging

def test_parse_unlinked_semantic_model(sample_project_dir: Path):
    """Test scenario where semantic model references a non-existent dbt model."""
    manifest_path = sample_project_dir / "target" / "manifest.json"
    # Modify semantic model to reference a bad model
    with open(sample_project_dir / "models" / "schema.yml", 'r+') as f:
        sm_data = yaml.safe_load(f)
        sm_data['semantic_models'][0]['model'] = "ref('non_existent_model')"
        f.seek(0)
        yaml.dump(sm_data, f)
        f.truncate()

    # Parsing should succeed, but the SM won't have '_model_unique_id'
    # Parser currently prints a warning.
    _metrics, semantic_models, _sql_map = parse_dbt_artifacts(
        dbt_project_dir=str(sample_project_dir),
        manifest_path=str(manifest_path)
    )
    assert len(semantic_models) == 1
    assert '_model_unique_id' not in semantic_models[0]
    # TODO: Capture and assert warnings if using logging

# TODO: Add more tests for edge cases:
# - Empty YAML files
# - Empty manifest
# - Different ref() styles
# - Multiple semantic models / metrics
