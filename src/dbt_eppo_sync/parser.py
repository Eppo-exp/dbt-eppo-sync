# dbt_eppo_sync/parser.py

import yaml
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

DbtMetric = Dict[str, Any]
DbtDimension = Dict[str, Any]
DbtEntity = Dict[str, Any]
DbtSemanticModel = Dict[str, Any]
CompiledSqlMap = Dict[str, str] # Maps dbt model unique_id to compiled SQL


class DbtParseError(Exception):
    """Custom exception for errors during dbt artifact parsing."""
    pass


# --- YAML Parsing Utilities ---

def load_yaml_file(file_path: Path) -> Optional[Dict[str, Any]]:
    """Loads a single YAML file."""
    if not file_path.is_file():
        return None
    try:
        with open(file_path, 'r') as f:
            content = yaml.safe_load(f)
            return content if content else None
    except yaml.YAMLError as e:
        raise DbtParseError(f"Error parsing YAML file {file_path}: {e}") from e
    except IOError as e:
        raise DbtParseError(f"Error reading file {file_path}: {e}") from e

def find_yaml_files(search_path: Path) -> List[Path]:
    """Finds all .yml and .yaml files recursively within a directory."""
    if not search_path.is_dir():
        raise DbtParseError(f"Provided YAML search path is not a directory: {search_path}")

    yaml_files = list(search_path.rglob('*.yml'))
    yaml_files.extend(list(search_path.rglob('*.yaml')))
    if not yaml_files:
        print(f"Warning: No YAML files found in {search_path}") # Use logging later
    return yaml_files

# --- Manifest Parsing Utilities ---

def load_manifest(manifest_path: Path) -> Dict[str, Any]:
    """
    Loads the dbt manifest.json file.

    Args:
        manifest_path: Path object pointing to the manifest.json file.

    Returns:
        A dictionary representing the parsed manifest content.

    Raises:
        DbtParseError: If the file cannot be found, read, or parsed as JSON.
    """
    if not manifest_path.is_file():
        raise DbtParseError(f"Manifest file not found at: {manifest_path}")

    try:
        with open(manifest_path, 'r') as f:
            manifest_content = json.load(f)
            if not isinstance(manifest_content, dict):
                 raise DbtParseError("Manifest content is not a valid JSON object.")
            return manifest_content
    except json.JSONDecodeError as e:
        raise DbtParseError(f"Error parsing JSON manifest file {manifest_path}: {e}") from e
    except IOError as e:
        raise DbtParseError(f"Error reading manifest file {manifest_path}: {e}") from e

def extract_compiled_sql(manifest: Dict[str, Any]) -> CompiledSqlMap:
    """
    Extracts compiled SQL code for models from the dbt manifest.

    Args:
        manifest: The parsed dbt manifest dictionary.

    Returns:
        A dictionary mapping dbt model unique_id to its compiled SQL string.
    """
    sql_map: CompiledSqlMap = {}
    nodes = manifest.get('nodes', {})

    if not nodes:
        print("Warning: No 'nodes' found in the manifest.") # Use logging
        return sql_map

    for unique_id, node_data in nodes.items():
        if isinstance(node_data, dict) and node_data.get('resource_type') == 'model':
            # Prefer 'compiled_code' (newer dbt versions), fallback to 'compiled_sql'
            compiled_sql = node_data.get('compiled_code') or node_data.get('compiled_sql')
            if compiled_sql and isinstance(compiled_sql, str):
                # Store the original compiled SQL
                sql_map[unique_id] = compiled_sql
            else:
                 print(f"Warning: Could not find compiled SQL for model node: {unique_id}") # Use logging

    if not sql_map:
         print("Warning: No compiled SQL found for any model nodes in the manifest.") # Use logging

    return sql_map

def _extract_ref_model_name(ref_string: str) -> Optional[str]:
    """
    Extracts the model name from a dbt ref() string like 'ref("model_name")'
    or 'ref("package", "model_name")'. Basic implementation.
    """
    # Simple regex to find model name inside ref() - might need improvement
    # Handles ref('model'), ref("model"), ref('pkg','model'), ref("pkg","model")
    match = re.search(r"""ref\(\s*(?:['"][\w\.]+['"]\s*,\s*)?['"]([\w\.]+)['"]\s*\)""", ref_string)
    if match:
        return match.group(1)
    return None

def find_model_unique_id(
    semantic_model: DbtSemanticModel,
    manifest_nodes: Dict[str, Any],
    dbt_project_name: Optional[str] = None # Helps resolve refs within the current project
) -> Optional[str]:
    """
    Finds the unique_id in the manifest corresponding to the dbt model
    referenced by a semantic model's 'model:' key.

    Args:
        semantic_model: The parsed semantic model dictionary.
        manifest_nodes: The 'nodes' dictionary from the parsed manifest.
        dbt_project_name: The name of the current dbt project (from dbt_project.yml).

    Returns:
        The unique_id of the corresponding model node, or None if not found.
    """
    model_ref_str = semantic_model.get('model')
    if not model_ref_str or not isinstance(model_ref_str, str):
        print(f"Warning: Semantic model '{semantic_model.get('name')}' is missing 'model' reference.")
        return None

    target_model_name = _extract_ref_model_name(model_ref_str)
    if not target_model_name:
         print(f"Warning: Could not parse model name from ref string: '{model_ref_str}' in semantic model '{semantic_model.get('name')}'")
         return None

    # Search for the model node in the manifest
    found_unique_id = None
    for unique_id, node_data in manifest_nodes.items():
        if (isinstance(node_data, dict) and
            node_data.get('resource_type') == 'model' and
            node_data.get('name') == target_model_name):
            # If multiple models have the same name (e.g., different packages),
            # prefer the one from the current project if project name is provided.
            if dbt_project_name and node_data.get('package_name') == dbt_project_name:
                return unique_id # Found exact match in current project
            # Otherwise, store the first match found
            if found_unique_id is None:
                found_unique_id = unique_id

    if found_unique_id:
        return found_unique_id
    else:
        print(f"Warning: Could not find model node '{target_model_name}' referenced by semantic model '{semantic_model.get('name')}' in the manifest.")
        return None


# --- Main Parsing Orchestrator ---

def parse_dbt_artifacts(
    dbt_project_dir: str, # Path to directory containing dbt_project.yml and models/semantics
    manifest_path: str    # Path to the compiled manifest.json
) -> Tuple[List[DbtMetric], List[DbtSemanticModel], CompiledSqlMap]:
    """
    Parses dbt semantic model/metric YAML files and the manifest.json
    to extract definitions and compiled SQL.

    Args:
        dbt_project_dir: Path to the root of the dbt project directory.
        manifest_path: Path to the target/manifest.json file.

    Returns:
        A tuple containing:
        - List of parsed dbt metrics.
        - List of parsed dbt semantic models (with '_model_unique_id' added).
        - Dictionary mapping model unique_ids to their compiled SQL.

    Raises:
        DbtParseError: If parsing fails or required files are not found.
    """
    project_path = Path(dbt_project_dir)
    manifest_file_path = Path(manifest_path)

    # --- Load Manifest and Extract SQL ---
    print(f"Loading manifest from: {manifest_file_path}")
    manifest = load_manifest(manifest_file_path)
    print("Extracting compiled SQL from manifest...")
    sql_map = extract_compiled_sql(manifest)
    print(f"Found compiled SQL for {len(sql_map)} models.")

    # Extract project name from manifest metadata (useful for resolving refs)
    dbt_project_name = manifest.get('metadata', {}).get('project_name')
    manifest_nodes = manifest.get('nodes', {})

    # --- Parse YAML definitions ---
    # Search for YAML files within the project dir (e.g., models/, semantics/)
    print(f"\nSearching for semantic YAML files in: {project_path}")
    yaml_files = find_yaml_files(project_path)

    all_metrics: List[DbtMetric] = []
    all_semantic_models: List[DbtSemanticModel] = []

    for file_path in yaml_files:
        print(f"Parsing YAML file: {file_path}") # Replace with logging
        content = load_yaml_file(file_path)
        if not content: continue

        # Extract Metrics
        if 'metrics' in content and isinstance(content['metrics'], list):
            for metric_data in content['metrics']:
                if isinstance(metric_data, dict) and 'name' in metric_data:
                    metric_data['_source_file'] = str(file_path)
                    all_metrics.append(metric_data)

        # Extract Semantic Models
        if 'semantic_models' in content and isinstance(content['semantic_models'], list):
             for sm_data in content['semantic_models']:
                if isinstance(sm_data, dict) and 'name' in sm_data:
                    sm_data['_source_file'] = str(file_path)
                    # --- Link Semantic Model to Manifest Node ---
                    model_unique_id = find_model_unique_id(sm_data, manifest_nodes, dbt_project_name)
                    if model_unique_id:
                        sm_data['_model_unique_id'] = model_unique_id
                        if model_unique_id not in sql_map:
                             print(f"Warning: Found unique_id '{model_unique_id}' for SM '{sm_data['name']}' but no compiled SQL in map.")
                    else:
                         print(f"Warning: Could not link semantic model '{sm_data['name']}' to a model in the manifest.")
                    # --------------------------------------------
                    all_semantic_models.append(sm_data)

    print(f"\nParsed {len(all_metrics)} metrics and {len(all_semantic_models)} semantic models from YAML files.")

    # Basic validation: Check if linked semantic models actually have SQL
    linked_sm_count = 0
    sm_with_sql_count = 0
    for sm in all_semantic_models:
        uid = sm.get('_model_unique_id')
        if uid:
            linked_sm_count += 1
            if uid in sql_map:
                sm_with_sql_count +=1

    print(f"Found {linked_sm_count} semantic models linked to manifest nodes.")
    print(f"Found compiled SQL for {sm_with_sql_count} of the linked semantic models.")
    if linked_sm_count > sm_with_sql_count:
        print("Warning: Some linked semantic models are missing compiled SQL in the manifest.")

    return all_metrics, all_semantic_models, sql_map


# --- Example Usage ---
if __name__ == '__main__':
    print("\n--- Running Parser Example ---")
    # Create dummy project structure and files for testing
    EXAMPLE_PROJECT_DIR = Path("./example_dbt_project")
    EXAMPLE_TARGET_DIR = EXAMPLE_PROJECT_DIR / "target"
    EXAMPLE_MODELS_DIR = EXAMPLE_PROJECT_DIR / "models"

    EXAMPLE_TARGET_DIR.mkdir(parents=True, exist_ok=True)
    EXAMPLE_MODELS_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Dummy dbt_project.yml (optional, but helps resolve project name)
    dummy_dbt_project_yaml = """
name: 'my_dbt_project'
version: '1.0'
profile: 'default'
model-paths: ["models"]
semantic-model-paths: ["models"] # Or wherever semantic models are
"""
    (EXAMPLE_PROJECT_DIR / "dbt_project.yml").write_text(dummy_dbt_project_yaml)


    # 2. Dummy manifest.json
    dummy_manifest = {
        "metadata": {"project_name": "my_dbt_project", "dbt_version": "1.x.x"},
        "nodes": {
            "model.my_dbt_project.stg_users": {
                "unique_id": "model.my_dbt_project.stg_users",
                "resource_type": "model",
                "name": "stg_users",
                "package_name": "my_dbt_project",
                "compiled": True,
                "compiled_code": "SELECT user_id, user_country, created_at FROM source_users -- compiled",
                "compiled_sql": "SELECT user_id, user_country, created_at FROM source_users -- compiled", # Older field
                "database": "db", "schema": "schema", "alias": "stg_users"
            },
             "model.my_dbt_project.orders": {
                "unique_id": "model.my_dbt_project.orders",
                "resource_type": "model",
                "name": "orders",
                "package_name": "my_dbt_project",
                "compiled": True,
                "compiled_code": "SELECT order_id, user_id, order_total FROM source_orders -- compiled",
                "database": "db", "schema": "schema", "alias": "orders"
            },
            "seed.my_dbt_project.country_codes": { # Example non-model node
                 "unique_id": "seed.my_dbt_project.country_codes",
                 "resource_type": "seed",
                 "name": "country_codes",
                 "package_name": "my_dbt_project",
            }
        },
        "sources": {}, "metrics": {}, "semantic_models": {}, "exposures": {}
    }
    manifest_file = EXAMPLE_TARGET_DIR / "manifest.json"
    manifest_file.write_text(json.dumps(dummy_manifest, indent=2))
    print(f"Created dummy manifest: {manifest_file}")

    # 3. Dummy semantic model YAML (referencing model 'stg_users')
    dummy_sm_yaml = """
semantic_models:
  - name: users # Semantic Model Name
    description: Represents individual users.
    model: ref('stg_users') # References dbt model name 'stg_users'
    entities:
      - name: user # Entity name
        type: primary
        expr: user_id # Column name for the entity key
    dimensions:
      - name: country # Dimension name
        type: categorical
        expr: user_country # Column/expression for the dimension
      - name: signup_date
        type: time
        expr: created_at::date
    measures:
      - name: number_of_users # Measure name
        agg: count_distinct # Aggregation type
        expr: user_id # Column/expression for the measure
"""
    (EXAMPLE_MODELS_DIR / "schema_users.yml").write_text(dummy_sm_yaml)
    print(f"Created dummy semantic model YAML: {EXAMPLE_MODELS_DIR / 'schema_users.yml'}")


    # 4. Dummy metrics YAML
    dummy_metrics_yaml = """
metrics:
  - name: total_users
    label: "Total Users"
    description: "The total count of distinct users."
    type: count_distinct # Corresponds to measure agg or metric type
    measure:
        name: number_of_users # References measure in 'users' semantic model
"""
    (EXAMPLE_MODELS_DIR / "metrics_users.yml").write_text(dummy_metrics_yaml)
    print(f"Created dummy metrics YAML: {EXAMPLE_MODELS_DIR / 'metrics_users.yml'}")


    # --- Execute Parsing ---
    print("\nAttempting to parse artifacts...")
    try:
        metrics, semantic_models, sql_map = parse_dbt_artifacts(
            dbt_project_dir=str(EXAMPLE_PROJECT_DIR),
            manifest_path=str(manifest_file)
        )

        print("\n--- Parsed Metrics ---")
        print(json.dumps(metrics, indent=2))

        print("\n--- Parsed Semantic Models (with _model_unique_id) ---")
        print(json.dumps(semantic_models, indent=2))

        print("\n--- Compiled SQL Map (unique_id -> SQL) ---")
        print(json.dumps(sql_map, indent=2))

    except DbtParseError as e:
        print(f"\nERROR during parsing: {e}")
    except Exception as e:
        print(f"\nUNEXPECTED ERROR during parsing example: {e}")
        import traceback
        traceback.print_exc()

    finally:
        # Clean up dummy files (optional)
        # import shutil
        # shutil.rmtree(EXAMPLE_PROJECT_DIR)
        # print(f"\nCleaned up dummy directory: {EXAMPLE_PROJECT_DIR}")
        pass
