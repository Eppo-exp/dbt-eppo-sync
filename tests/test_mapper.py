# tests/test_mapper.py

import pytest
from dbt_eppo_sync.mapper import map_dbt_to_eppo_sync_payload, DbtMappingError

# --- Sample Inputs (Define directly or load from fixtures) ---

SAMPLE_SEMANTIC_MODELS = [
    {
        'name': 'users', '_model_unique_id': 'model.proj.dim_users',
        'entities': [{'name': 'user', 'type': 'primary', 'expr': 'user_id'}],
        'dimensions': [
            {'name': 'country', 'type': 'categorical', 'expr': 'country_code'},
            {'name': 'signup_ts', 'type': 'time', 'expr': 'created_at'} # Timestamp
        ],
        'measures': [
            {'name': 'user_count', 'agg': 'count_distinct', 'expr': 'user_id'},
            {'name': 'revenue', 'agg': 'sum', 'expr': 'order_total', 'meta': {'desired_change': 'increase'}}
        ]
    }
]
SAMPLE_METRICS = [
    {
        'name': 'total_revenue', 'label': 'Total Revenue', 'type': 'sum',
        'measure': {'name': 'revenue'}
    },
    {
        'name': 'avg_revenue', 'label': 'Avg Revenue', 'type': 'average',
        'measure': {'name': 'revenue'} # Will be mapped to ratio(sum(revenue)/count(revenue))
    },
    {
        'name': 'revenue_ca', 'label': 'Revenue (CA)', 'type': 'sum',
        'measure': {'name': 'revenue'},
        'filter': "{{ Dimension('users__user__country') }} = 'CA'"
    },
    {
        'name': 'p95_revenue', 'label': 'P95 Revenue', 'type': 'percentile',
        'measure': {'name': 'revenue'}, 'percentile': 0.95
    }
]
SAMPLE_SQL_MAP = {
    'model.proj.dim_users': "SELECT user_id, country_code, created_at, order_total FROM source_users"
}

# --- Test Cases ---

def test_map_successful():
    """Test basic successful mapping."""
    payload = map_dbt_to_eppo_sync_payload(
        dbt_metrics=SAMPLE_METRICS,
        dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
        sql_map=SAMPLE_SQL_MAP,
        sync_tag="test-run"
    )

    assert payload['sync_tag'] == "test-run"
    assert len(payload['fact_sources']) == 1
    assert len(payload['metrics']) == 4 # total_revenue, avg_revenue, revenue_ca, p95_revenue

    # --- Validate Fact Source ---
    fs = payload['fact_sources'][0]
    assert fs['name'] == 'users'
    assert fs['sql'] == SAMPLE_SQL_MAP['model.proj.dim_users']
    assert fs['timestamp_column'] == 'created_at'
    assert len(fs['entities']) == 1 and fs['entities'][0]['entity_name'] == 'user'
    assert len(fs['facts']) == 2 # user_count, revenue
    assert fs['facts'][1]['name'] == 'revenue' # Maps dbt measure name
    assert fs['facts'][1]['column'] == 'order_total'
    assert fs['facts'][1]['desired_change'] == 'increase'
    assert len(fs['properties']) == 2 # country, signup_ts (timestamp is also a property)
    assert fs['properties'][0]['name'] == 'country'

    # --- Validate Metrics (Spot Check) ---
    eppo_metrics = {m['name']: m for m in payload['metrics']}

    # Simple Sum
    assert 'Total Revenue' in eppo_metrics
    tr = eppo_metrics['Total Revenue']
    assert tr['type'] == 'simple'
    assert tr['entity'] == 'user'
    assert tr['numerator']['fact_name'] == 'revenue' # Matches fact name derived from measure
    assert tr['numerator']['operation'] == 'sum'

    # Average (mapped to Ratio)
    assert 'Avg Revenue' in eppo_metrics
    ar = eppo_metrics['Avg Revenue']
    assert ar['type'] == 'ratio'
    assert ar['entity'] == 'user'
    assert ar['numerator']['fact_name'] == 'revenue'
    assert ar['numerator']['operation'] == 'sum'
    assert ar['denominator']['fact_name'] == 'revenue' # Uses same fact
    assert ar['denominator']['operation'] == 'count' # Count non-null values

    # Filtered
    assert 'Revenue (CA)' in eppo_metrics
    rca = eppo_metrics['Revenue (CA)']
    assert rca['type'] == 'simple'
    assert rca['numerator']['operation'] == 'sum'
    assert len(rca['numerator']['filters']) == 1
    assert rca['numerator']['filters'][0]['fact_property'] == 'country' # Matches property name
    assert rca['numerator']['filters'][0]['operation'] == 'equals'
    assert rca['numerator']['filters'][0]['values'] == ['CA']

    # Percentile
    assert 'P95 Revenue' in eppo_metrics
    p95 = eppo_metrics['P95 Revenue']
    assert p95['type'] == 'percentile'
    assert p95['percentile']['fact_name'] == 'revenue'
    assert p95['percentile']['percentile_value'] == 0.95
    assert 'operation' not in p95['percentile'] # Operation not valid here


def test_map_missing_sql():
    """Test error when compiled SQL is missing for a linked SM."""
    with pytest.raises(DbtMappingError, match="Could not find compiled SQL"):
         map_dbt_to_eppo_sync_payload(
            dbt_metrics=SAMPLE_METRICS,
            dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
            sql_map={}, # Empty SQL map
            sync_tag="test-run"
        )

def test_map_missing_timestamp():
    """Test error when timestamp column cannot be found."""
    sm_no_ts = [{
        'name': 'users', '_model_unique_id': 'model.proj.dim_users',
        'entities': [{'name': 'user', 'type': 'primary', 'expr': 'user_id'}],
        'dimensions': [{'name': 'country', 'type': 'categorical', 'expr': 'country_code'}], # No time dim
        'measures': [{'name': 'user_count', 'agg': 'count_distinct', 'expr': 'user_id'}]
    }]
    with pytest.raises(DbtMappingError, match="Could not automatically identify a required timestamp column"):
         map_dbt_to_eppo_sync_payload(
            dbt_metrics=[],
            dbt_semantic_models=sm_no_ts,
            sql_map=SAMPLE_SQL_MAP,
            sync_tag="test-run"
        )

def test_map_missing_measure_link():
    """Test error when a metric references a measure not found in SMs."""
    bad_metrics = [{
        'name': 'bad_metric', 'label': 'Bad Metric', 'type': 'sum',
        'measure': {'name': 'non_existent_measure'} # This measure isn't in SAMPLE_SEMANTIC_MODELS
    }]
    # This should ideally raise an error during mapping when linking fails
    with pytest.raises(DbtMappingError, match="Could not find Eppo fact mapping for primary measure 'non_existent_measure'"):
        map_dbt_to_eppo_sync_payload(
            dbt_metrics=bad_metrics,
            dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
            sql_map=SAMPLE_SQL_MAP,
            sync_tag="test-run"
        )

# TODO: Add more tests:
# - Mapping of optional fields from meta tags (display_style, is_guardrail etc.)
# - Different filter types (not_equals) and unmappable filters
# - Edge cases: No metrics, no semantic models (should still produce valid empty lists)
# - Complex ratio metrics with different denominator measures
# - Measures without 'expr' (potential row counts) - requires mapper adjustment
