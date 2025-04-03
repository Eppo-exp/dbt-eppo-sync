# dbt_eppo_sync/mapper.py

import datetime
import re
import json # Added for example usage clarity
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

# Import types from the updated parser
try:
    # Use relative import if running as part of the package
    from .parser import DbtMetric, DbtSemanticModel, CompiledSqlMap, DbtParseError
except ImportError:
    # Fallback for running script directly or testing
    print("Warning: Running mapper.py with potential direct imports.")
    DbtMetric = Dict[str, Any]
    DbtSemanticModel = Dict[str, Any]
    CompiledSqlMap = Dict[str, str]
    class DbtParseError(Exception): pass # Define dummy exception


class DbtMappingError(Exception):
    """Custom exception for errors during dbt to Eppo mapping."""
    pass

# --- Helper Functions for Mapping Sub-structures ---

def _get_meta_value(obj: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Safely retrieves a value from the 'meta' dictionary."""
    return obj.get('meta', {}).get(key, default)

def _map_dbt_entities_to_eppo(dbt_entities: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """Maps dbt entities to Eppo fact_source entities."""
    eppo_entities = []
    # Find the primary entity
    primary_entity = next((e for e in dbt_entities if e.get('type') == 'primary'), None)
    if primary_entity and 'name' in primary_entity and 'expr' in primary_entity:
        eppo_entities.append({
            "entity_name": primary_entity['name'], # e.g., 'user'. NOTE: Schema says this must match Eppo UI entity name exactly.
            "column": primary_entity['expr']      # e.g., 'user_id'
        })
    else:
        # Maybe log a warning if no primary entity is found? Or should this be an error?
        # Depending on Eppo requirements, this might be critical.
        print(f"Warning: No primary entity found or missing name/expr in: {dbt_entities}")
        # Add logic for foreign keys if Eppo schema supports/requires them
    if not eppo_entities:
         raise DbtMappingError(f"Could not map any primary entity from dbt entities: {dbt_entities}")
    return eppo_entities

def _map_dbt_measures_to_eppo_facts(dbt_measures: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """
    Maps dbt measures to Eppo facts within a fact_source.
    These represent values/events available from the source SQL.

    Returns:
        A tuple containing:
        - List of Eppo fact definitions for the fact_source.
        - A dictionary mapping dbt measure name to its corresponding Eppo fact name.
    """
    eppo_facts = []
    measure_to_fact_map = {}
    for measure in dbt_measures:
        # Eppo facts: name, optional column, description, optional desired_change
        # If 'column' is null/omitted, Eppo uses the record itself (e.g., for counts).
        # We need a way to distinguish measures that represent values (need 'expr'/'column')
        # from measures that represent events/rows (e.g., simple count, might not need 'expr').

        measure_name = measure.get('name')
        if not measure_name:
            print(f"Warning: Skipping measure due to missing name: {measure}")
            continue

        # Use measure name directly as Eppo fact name for simplicity in linking
        # Ensure this name is unique within the fact_source.
        eppo_fact_name = measure_name

        # Determine column: Use 'expr' if present. How to handle row counts?
        # If measure is simple count (agg: count) and no expr? Assume column=None?
        # For now, require 'expr' to map to a fact with a column.
        column_expr = measure.get('expr')
        # if not column_expr and measure.get('agg') == 'count':
        #     # Potentially map to a fact with column=None for row counts
        #     pass # Requires adjustment
        # elif not column_expr:
        #      print(f"Warning: Skipping measure '{measure_name}' due to missing expr.")
        #      continue # Skip if we need an expression but don't have one

        fact_payload = {
            "name": eppo_fact_name,
            "description": measure.get('description', ''),
        }
        # Only include column if expr exists
        if column_expr:
             fact_payload["column"] = column_expr

        # Map desired_change from meta, default to increase
        desired_change = _get_meta_value(measure, 'eppo_desired_change', 'increase')
        if desired_change in ['increase', 'decrease']:
            fact_payload["desired_change"] = desired_change

        eppo_facts.append(fact_payload)
        measure_to_fact_map[measure_name] = eppo_fact_name # Map dbt measure name to Eppo fact name

    return eppo_facts, measure_to_fact_map

def _map_dbt_dimensions_to_eppo_properties(dbt_dimensions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Maps dbt dimensions to Eppo properties within a fact_source."""
    eppo_properties = []
    for dim in dbt_dimensions:
        if 'name' in dim and 'expr' in dim:
            prop_payload = {
                "name": dim['name'], # NOTE: Schema filter description implies this must match property name used in filters.
                "column": dim['expr'],
                "description": dim.get('description', ''),
            }
            eppo_properties.append(prop_payload)
        else:
             print(f"Warning: Skipping dimension due to missing name or expr: {dim}")
    return eppo_properties

def _find_timestamp_column(dbt_dimensions: List[Dict[str, Any]], semantic_model_name: str) -> str:
    """
    Attempts to find a suitable timestamp column from dbt dimensions.
    Raises DbtMappingError if none found, as it's required by Eppo schema.
    """
    for dim in dbt_dimensions:
        dim_expr = dim.get('expr')
        if not dim_expr: continue

        # Look for dimensions of type 'time' or common names
        if dim.get('type') == 'time':
            return dim_expr
        # Prioritize common names if type='time' isn't used
        if dim.get('name') in ['timestamp', 'event_timestamp', 'ts', 'created_at', 'updated_at']:
             return dim_expr

    # If no specific timestamp found, maybe check entities? Unlikely standard.

    # If still not found, raise error as it's required
    raise DbtMappingError(f"Could not automatically identify a required timestamp column for semantic model '{semantic_model_name}'. "
                          "Ensure a dimension has type 'time' or a common timestamp name (e.g., 'timestamp', 'created_at').")


def _map_dbt_filter_to_eppo(dbt_filter_str: str, fact_properties: List[Dict[str, Any]], sm_name: str) -> List[Dict[str, Any]]:
    """
    Attempts to map a dbt filter string to Eppo filter objects.
    Handles simple equality and inequality.
    Example dbt filter: "{{ Dimension('users__user__country') }} = 'CA'"
                        "{{ Dimension('users__user__status') }} != 'Inactive'"
    Example Eppo filter: { "fact_property": "country", "operation": "equals", "values": ["CA"] }
                         { "fact_property": "status", "operation": "not_equals", "values": ["Inactive"] }
    """
    # Regex for: dimension_ref operator 'Value' or "Value"
    # Handles =, !=
    match = re.match(r"\s*\{\{\s*Dimension\s*\(\s*['\"]([\w_]+)['\"]\s*\)\s*\}\}\s*(!=|==|=)\s*['\"]([^'\"]+)['\"]\s*", dbt_filter_str)

    if match:
        dbt_dim_ref = match.group(1) # e.g., users__user__country
        operator = match.group(2).strip()
        value = match.group(3)

        # Map operator
        if operator == '=' or operator == '==':
            eppo_op = 'equals'
        elif operator == '!=':
            eppo_op = 'not_equals'
        else:
            print(f"Warning: Unsupported filter operator '{operator}' in filter: '{dbt_filter_str}'")
            return []

        # Attempt to map dbt_dim_ref to an Eppo property name
        target_dim_name_parts = dbt_dim_ref.split('__')
        target_dim_name = target_dim_name_parts[-1] # Guess: last part is the dim name

        property_name = None
        for prop in fact_properties:
            if prop.get('name') == target_dim_name:
                property_name = prop['name']
                break

        if property_name:
            return [{
                "fact_property": property_name,
                "operation": eppo_op,
                "values": [value] # Eppo expects an array of strings
            }]
        else:
            print(f"Warning: Could not map filter dimension '{target_dim_name}' (from {dbt_dim_ref}) in SM '{sm_name}' to a known fact property.")

    print(f"Warning: Could not parse or map dbt filter string: '{dbt_filter_str}'")
    return []


def _map_dbt_aggregation_to_eppo_op(dbt_agg: Optional[str], context: str) -> str:
    """Maps dbt aggregation function names to Eppo operation names."""
    if not dbt_agg:
         raise DbtMappingError(f"Missing aggregation type for {context}")

    agg_lower = dbt_agg.lower()
    # Eppo operations enum: "sum", "count", "distinct_entity", "threshold", "conversion", "retention", "count_distinct", "last_value", "first_value"
    if agg_lower == 'sum':
        return 'sum'
    elif agg_lower == 'count':
        return 'count'
    elif agg_lower == 'count_distinct':
        # Map to Eppo's count_distinct or distinct_entity? Check Eppo docs for semantic difference.
        # Using 'distinct_entity' as it's present in the schema enum.
        return 'distinct_entity'
    elif agg_lower in ['average', 'avg', 'mean']:
        # Average metrics should be handled as Eppo type "ratio"
        raise DbtMappingError(f"dbt aggregation '{agg_lower}' should be mapped as an Eppo 'ratio' metric type, not a simple operation. Context: {context}")
    # Add mappings for min, max if needed and supported by Eppo ops (e.g., first_value/last_value?)
    else:
        raise DbtMappingError(f"Unsupported dbt aggregation '{dbt_agg}' for Eppo operation mapping. Context: {context}")

# --- Main Mapping Function ---

def map_dbt_to_eppo_sync_payload(
    dbt_metrics: List[DbtMetric],
    dbt_semantic_models: List[DbtSemanticModel],
    sql_map: CompiledSqlMap,
    sync_tag: Optional[str] = None,
    reference_url_base: Optional[str] = None # e.g., base URL for linking back to git repo
) -> Dict[str, Any]:
    """
    Maps parsed dbt artifacts to the Eppo bulk sync API payload structure,
    aligned with the official schema.

    Args:
        dbt_metrics: List of parsed dbt metrics.
        dbt_semantic_models: List of parsed dbt semantic models (linked via _model_unique_id).
        sql_map: Dictionary mapping model unique_ids to compiled SQL.
        sync_tag: An optional tag for the sync operation.
        reference_url_base: Optional base URL for constructing reference links.

    Returns:
        A dictionary formatted for the Eppo '/api/v1/metrics/sync' endpoint.

    Raises:
        DbtMappingError: If critical mapping information is missing or invalid.
    """
    eppo_fact_sources = []
    # Keep track of mappings for linking metrics later
    # Map (sm_name, dbt_measure_name) -> eppo_fact_name
    dbt_measure_to_eppo_fact_map: Dict[Tuple[str, str], str] = {}
    # Map sm_name -> list of eppo property dicts defined for it
    sm_properties_map: Dict[str, List[Dict[str, Any]]] = {}
    # Map sm_name -> primary entity name
    sm_primary_entity_map: Dict[str, str] = {}
    # Map sm_name -> list of dbt measure definitions
    sm_measures_map: Dict[str, List[Dict[str, Any]]] = {}


    # --- 1. Process Semantic Models into Fact Sources ---
    print("\nMapping Semantic Models to Eppo Fact Sources...")
    for sm in dbt_semantic_models:
        sm_name = sm.get('name')
        if not sm_name:
            print("Warning: Skipping semantic model with no name.")
            continue

        model_unique_id = sm.get('_model_unique_id')
        if not model_unique_id:
            print(f"Warning: Skipping semantic model '{sm_name}' because it's not linked to a manifest node.")
            continue

        compiled_sql = sql_map.get(model_unique_id)
        if not compiled_sql:
            # This could be an error depending on strictness, but maybe some SMs aren't meant to be synced
            print(f"Warning: Skipping semantic model '{sm_name}' (node: {model_unique_id}) because compiled SQL is missing.")
            continue

        print(f"  Processing Semantic Model: {sm_name} (using SQL from {model_unique_id})")

        try:
            # Extract components
            dbt_entities = sm.get('entities', [])
            dbt_measures = sm.get('measures', [])
            dbt_dimensions = sm.get('dimensions', [])

            # Map sub-structures
            eppo_entities = _map_dbt_entities_to_eppo(dbt_entities) # Raises if no primary
            eppo_facts, measure_to_fact_map = _map_dbt_measures_to_eppo_facts(dbt_measures)
            eppo_properties = _map_dbt_dimensions_to_eppo_properties(dbt_dimensions)
            timestamp_col = _find_timestamp_column(dbt_dimensions, sm_name) # Raises if not found

            # Store mapping info for later use by metrics
            for measure_name, fact_name in measure_to_fact_map.items():
                dbt_measure_to_eppo_fact_map[(sm_name, measure_name)] = fact_name
            sm_properties_map[sm_name] = eppo_properties # Store full property dicts
            sm_primary_entity_map[sm_name] = eppo_entities[0]['entity_name'] # Assumes first is primary
            sm_measures_map[sm_name] = dbt_measures # Store original measures for lookup

            # Construct Eppo Fact Source object
            fact_source = {
                "name": sm_name, # Use semantic model name as fact source name
                "sql": compiled_sql,
                "timestamp_column": timestamp_col, # Now required
                "entities": eppo_entities,
                "facts": eppo_facts,
                "properties": eppo_properties,
            }
            # Add optional fields from meta
            ref_url = _get_meta_value(sm, 'eppo_reference_url')
            if ref_url: fact_source['reference_url'] = ref_url
            full_refresh = _get_meta_value(sm, 'eppo_always_full_refresh')
            if isinstance(full_refresh, bool): fact_source['always_full_refresh'] = full_refresh

            eppo_fact_sources.append(fact_source)

        except DbtMappingError as map_err:
             print(f"  ERROR processing semantic model '{sm_name}': {map_err}. Skipping this fact source.")
        except Exception as e:
             print(f"  UNEXPECTED ERROR processing semantic model '{sm_name}': {e}. Skipping this fact source.")
             import traceback
             traceback.print_exc()


    # --- 2. Process dbt Metrics into Eppo Metrics ---
    print("\nMapping dbt Metrics to Eppo Metrics...")
    eppo_metrics = []
    for metric in dbt_metrics:
        metric_name = metric.get('name')
        if not metric_name:
            print("Warning: Skipping metric with no name.")
            continue

        print(f"  Processing dbt Metric: {metric_name}")
        try:
            # --- Determine Eppo Metric Type ---
            # Eppo types: "simple", "ratio", "funnel", "percentile"
            dbt_type = metric.get('type', '').lower()
            eppo_metric_type = None
            is_average_metric = False

            if dbt_type == 'ratio' or (dbt_type == 'derived' and metric.get('numerator') and metric.get('denominator')):
                 eppo_metric_type = "ratio"
            elif dbt_type in ['sum', 'count', 'count_distinct', 'median']: # Median might map to percentile:0.5 or simple? Assume simple for now.
                 eppo_metric_type = "simple"
            elif dbt_type in ['average', 'mean']:
                 eppo_metric_type = "ratio" # Represent average as ratio(sum/count)
                 is_average_metric = True
            elif dbt_type == 'derived' and metric.get('measure'): # Simple derived from single measure
                 eppo_metric_type = "simple"
            elif metric.get('measure'): # Assume simple if type is missing but measure is present
                 print(f"Warning: Metric '{metric_name}' has no type, assuming 'simple'.")
                 eppo_metric_type = "simple"
            else:
                raise DbtMappingError(f"Cannot determine Eppo metric type for dbt metric definition.")

            # --- Find Referenced Eppo Fact(s) and Entity ---
            # Common logic: find the primary measure/fact and its source SM/entity
            primary_measure_ref = metric.get('measure') or metric.get('numerator', {}).get('measure')
            if not isinstance(primary_measure_ref, dict) or 'name' not in primary_measure_ref:
                 raise DbtMappingError(f"Missing primary 'measure' reference in metric or its numerator.")

            primary_measure_name = primary_measure_ref['name']
            primary_fact_name = None
            source_sm_name = None
            primary_entity_name = None

            # Find the mapping for the primary measure
            found_mapping = None
            for (sm_n, m_n), fact_n in dbt_measure_to_eppo_fact_map.items():
                if m_n == primary_measure_name:
                    found_mapping = (sm_n, fact_n)
                    break
            if found_mapping:
                source_sm_name, primary_fact_name = found_mapping
                primary_entity_name = sm_primary_entity_map.get(source_sm_name)
                if not primary_entity_name:
                     raise DbtMappingError(f"Could not determine primary entity for source SM '{source_sm_name}'.")
            else:
                raise DbtMappingError(f"Could not find Eppo fact mapping for primary measure '{primary_measure_name}'. Was the semantic model processed?")

            # --- Construct Numerator ---
            # Required for all types except maybe funnel?
            numerator_payload = {"fact_name": primary_fact_name}

            # Determine operation for simple/ratio numerator
            if eppo_metric_type in ["simple", "ratio"]:
                if is_average_metric:
                     # Numerator for average is SUM
                     numerator_payload["operation"] = "sum"
                else:
                    # Find original dbt measure definition to get aggregation
                    dbt_measure_def = next((m for m in sm_measures_map.get(source_sm_name, []) if m.get('name') == primary_measure_name), None)
                    if not dbt_measure_def:
                         raise DbtMappingError(f"Could not find original dbt measure definition for '{primary_measure_name}' in SM '{source_sm_name}'.")
                    dbt_agg = dbt_measure_def.get('agg')
                    # Map dbt agg to Eppo op, handling average case specifically
                    try:
                         numerator_payload["operation"] = _map_dbt_aggregation_to_eppo_op(dbt_agg, f"metric '{metric_name}' numerator")
                    except DbtMappingError as agg_err:
                         # If it's an average, we expect this error, otherwise re-raise
                         if not (is_average_metric and 'average' in str(agg_err)):
                             raise agg_err
                         # We already set operation to 'sum' for average numerator
                         pass


            # --- Construct Denominator (for Ratio type) ---
            denominator_payload = None
            if eppo_metric_type == "ratio":
                den_fact_name = None
                den_operation = None
                if is_average_metric:
                    # Denominator for average is COUNT
                    # Use the same fact as numerator (representing the value column)
                    den_fact_name = primary_fact_name
                    den_operation = "count" # Count the non-null values of the fact column
                else:
                    # Find denominator measure reference from dbt metric
                    den_measure_ref = metric.get('denominator', {}).get('measure')
                    if not isinstance(den_measure_ref, dict) or 'name' not in den_measure_ref:
                         raise DbtMappingError(f"Ratio metric '{metric_name}' is missing denominator measure reference.")
                    den_measure_name = den_measure_ref['name']
                    # Find the corresponding Eppo fact name
                    found_den_mapping = None
                    den_source_sm = None
                    for (sm_n, m_n), fact_n in dbt_measure_to_eppo_fact_map.items():
                         if m_n == den_measure_name:
                             found_den_mapping = (sm_n, fact_n)
                             break
                    if found_den_mapping:
                        den_source_sm, den_fact_name = found_den_mapping
                    else:
                         raise DbtMappingError(f"Could not find Eppo fact mapping for denominator measure '{den_measure_name}'.")

                    # Find original dbt measure definition for denominator to get aggregation
                    den_dbt_measure_def = next((m for m in sm_measures_map.get(den_source_sm, []) if m.get('name') == den_measure_name), None)
                    if not den_dbt_measure_def:
                         raise DbtMappingError(f"Could not find original dbt measure definition for denominator '{den_measure_name}' in SM '{den_source_sm}'.")
                    den_dbt_agg = den_dbt_measure_def.get('agg')
                    den_operation = _map_dbt_aggregation_to_eppo_op(den_dbt_agg, f"metric '{metric_name}' denominator")

                denominator_payload = {
                    "fact_name": den_fact_name,
                    "operation": den_operation
                }

            # --- Map Filters (apply to num/den/perc) ---
            dbt_filter = metric.get('filter')
            eppo_filters = []
            if dbt_filter and source_sm_name and source_sm_name in sm_properties_map:
                eppo_filters = _map_dbt_filter_to_eppo(dbt_filter, sm_properties_map[source_sm_name], source_sm_name)

            if eppo_filters:
                if numerator_payload: numerator_payload["filters"] = eppo_filters
                if denominator_payload: denominator_payload["filters"] = eppo_filters # Apply same filters? Check Eppo logic


            # --- Construct Eppo Metric Object ---
            eppo_metric = {
                "name": metric.get('label') or metric_name, # Prefer label for display name
                "description": metric.get('description', ''),
                "entity": primary_entity_name,
                # Optional fields from meta
                "is_guardrail": _get_meta_value(metric, 'eppo_is_guardrail'),
                "metric_display_style": _get_meta_value(metric, 'eppo_display_style'),
                "minimum_detectable_effect": _get_meta_value(metric, 'eppo_mde'),
                "reference_url": _get_meta_value(metric, 'eppo_reference_url'),
                # Required/Optional structures
                "numerator": numerator_payload,
                "denominator": denominator_payload,
            }
            # Clean up None values / empty structures
            eppo_metric = {k: v for k, v in eppo_metric.items() if v is not None} # Remove None optional fields
            if not eppo_metric.get("numerator"): eppo_metric.pop("numerator", None)
            if not eppo_metric.get("denominator"): eppo_metric.pop("denominator", None)

            # Validate required fields based on type
            if 'numerator' not in eppo_metric: # Removed check for != 'percentile'
                 raise DbtMappingError(f"Numerator payload is missing for metric '{metric_name}' of type '{eppo_metric_type}'.")
            if eppo_metric_type == 'ratio' and 'denominator' not in eppo_metric:
                 raise DbtMappingError(f"Denominator payload is missing for ratio metric '{metric_name}'.")


            eppo_metrics.append(eppo_metric)

        except DbtMappingError as e:
            print(f"  ERROR mapping metric '{metric_name}': {e}") # Log and continue
        except Exception as e:
             print(f"  UNEXPECTED ERROR mapping metric '{metric_name}': {e}") # Log and continue
             import traceback
             traceback.print_exc()


    # --- 3. Assemble Final Payload ---
    final_payload = {
        "sync_tag": sync_tag or f"dbt-sync-{datetime.datetime.utcnow().isoformat()}",
        # "reference_url": "", # Optional top-level reference - maybe link to repo?
        "fact_sources": eppo_fact_sources,
        "metrics": eppo_metrics
    }

    print(f"\nFinished mapping. Generated {len(eppo_fact_sources)} fact sources and {len(eppo_metrics)} metrics.")
    return final_payload


# --- Example Usage ---
if __name__ == '__main__':
    print("\n--- Running Mapper Example (v3 - Aligned with Schema) ---")

    EXAMPLE_PROJECT_DIR = Path("./example_dbt_project")
    EXAMPLE_TARGET_DIR = EXAMPLE_PROJECT_DIR / "target"
    manifest_file = EXAMPLE_TARGET_DIR / "manifest.json"

    if not manifest_file.exists():
         print(f"Error: Dummy manifest file not found at {manifest_file}. Run parser.py example first.")
    else:
        try:
            # 1. Parse artifacts using the updated parser
            print("Parsing artifacts using parser_v2...")
            from parser import parse_dbt_artifacts # Assumes parser.py is runnable
            metrics, semantic_models, sql_map = parse_dbt_artifacts(
                dbt_project_dir=str(EXAMPLE_PROJECT_DIR),
                manifest_path=str(manifest_file)
            )

            # Add more measures/metrics to test different types
            # Find the 'users' semantic model to add measures
            users_sm = next((sm for sm in semantic_models if sm['name'] == 'users'), None)
            if users_sm:
                if not any(m['name'] == 'total_revenue' for m in users_sm.get('measures',[])):
                    users_sm.setdefault('measures', []).append(
                        {'name': 'total_revenue', 'agg': 'sum', 'expr': 'order_revenue', 'meta': {'desired_change': 'increase'}}
                    )
                if not any(m['name'] == 'is_converted' for m in users_sm.get('measures',[])):
                     users_sm.setdefault('measures', []).append(
                         {'name': 'is_converted', 'agg': 'sum', 'expr': 'conversion_flag'} # Assuming binary 0/1 flag
                     )
                # Add a timestamp dimension if missing for testing _find_timestamp_column
                if not any(d.get('type') == 'time' for d in users_sm.get('dimensions', [])):
                     print("Adding dummy timestamp dimension 'created_at' to users SM for testing.")
                     users_sm.setdefault('dimensions', []).append(
                         {'name': 'created_at', 'type': 'time', 'expr': 'created_at'}
                     )


            # Add/Update metric examples
            metrics = [m for m in metrics if m['name'] not in ['avg_revenue_per_user', 'user_conversion_rate', 'total_users_ca']] # Remove old test metrics first

            # Average (should become ratio)
            metrics.append({
                'name': 'avg_revenue_per_user', 'label': 'Avg Revenue Per User', 'type': 'average',
                'measure': {'name': 'total_revenue'}, # Based on sum measure
                'meta': {'eppo_display_style': 'decimal'}
            })
            # Ratio
            metrics.append({
                'name': 'user_conversion_rate', 'label': 'User Conversion Rate', 'type': 'ratio',
                'numerator': {'measure': 'is_converted'}, # Sum of conversion flags
                'denominator': {'measure': 'number_of_users'}, # Count distinct users
                'meta': {'eppo_display_style': 'percent', 'eppo_is_guardrail': False}
            })
             # Filtered metric
            metrics.append({
                'name': 'total_users_ca', 'label': 'Total Users (CA)', 'type': 'count_distinct',
                'measure': {'name': 'number_of_users'},
                'filter': "{{ Dimension('users__user__country') }} = 'CA'",
            })
             # Percentile metric
            metrics.append({
                'name': 'p95_revenue', 'label': 'P95 Revenue', 'type': 'percentile',
                'measure': {'name': 'total_revenue'},
                'percentile': 0.95 # Specify percentile value
            })


            # 2. Map to Eppo payload
            print("\nMapping parsed artifacts to Eppo sync payload...")
            eppo_payload = map_dbt_to_eppo_sync_payload(
                dbt_metrics=metrics,
                dbt_semantic_models=semantic_models,
                sql_map=sql_map,
                sync_tag="test-sync-aligned-schema"
            )

            print("\n--- Generated Eppo Sync Payload (Aligned) ---")
            print(json.dumps(eppo_payload, indent=2))

            # Basic validation checks on payload structure
            assert 'sync_tag' in eppo_payload
            assert 'fact_sources' in eppo_payload
            assert 'metrics' in eppo_payload
            if eppo_payload['fact_sources']:
                 assert 'timestamp_column' in eppo_payload['fact_sources'][0]
                 assert eppo_payload['fact_sources'][0]['timestamp_column'] is not None # Check it's required
            print("\nPayload structure seems valid based on basic checks.")

        except DbtParseError as e:
             print(f"\nERROR during parsing step: {e}")
        except DbtMappingError as e:
            print(f"\nERROR during mapping step: {e}")
        except ImportError:
             print("\nERROR: Could not import parser. Make sure parser.py is in the same directory or installed.")
        except FileNotFoundError:
             print(f"\nERROR: Example files not found in {EXAMPLE_PROJECT_DIR}. Run parser.py example first.")
        except Exception as e:
            print(f"\nUNEXPECTED ERROR during mapper example: {e}")
            import traceback
            traceback.print_exc()

