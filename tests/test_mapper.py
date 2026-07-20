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
    assert tr['entity'] == 'user'
    assert tr['numerator']['fact_name'] == 'revenue' # Matches fact name derived from measure
    assert tr['numerator']['operation'] == 'sum'
    assert 'denominator' not in tr  # Simple metrics don't have denominators

    # Average (mapped to Ratio)
    assert 'Avg Revenue' in eppo_metrics
    ar = eppo_metrics['Avg Revenue']
    assert ar['entity'] == 'user'
    assert ar['numerator']['fact_name'] == 'revenue'
    assert ar['numerator']['operation'] == 'sum'
    assert ar['denominator']['fact_name'] == 'revenue' # Uses same fact
    assert ar['denominator']['operation'] == 'count' # Count non-null values

    # Filtered
    assert 'Revenue (CA)' in eppo_metrics
    rca = eppo_metrics['Revenue (CA)']
    assert rca['numerator']['operation'] == 'sum'
    assert len(rca['numerator']['filters']) == 1
    assert rca['numerator']['filters'][0]['fact_property'] == 'country' # Matches property name
    assert rca['numerator']['filters'][0]['operation'] == 'equals'
    assert rca['numerator']['filters'][0]['values'] == ['CA']

    # Percentile - Currently treated as simple metric (not fully supported)
    assert 'P95 Revenue' in eppo_metrics
    p95 = eppo_metrics['P95 Revenue']
    assert p95['entity'] == 'user'
    assert p95['numerator']['fact_name'] == 'revenue'
    assert p95['numerator']['operation'] == 'sum'  # Currently mapped to simple sum
    # Note: Percentile-specific fields (percentile value) are not currently supported


def test_map_sync_level_creator_updater_team():
    """Sync-level creator_email, updater_email, team_name appear in payload when provided."""
    payload = map_dbt_to_eppo_sync_payload(
        dbt_metrics=SAMPLE_METRICS,
        dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
        sql_map=SAMPLE_SQL_MAP,
        sync_tag="test-run",
        creator_email="data-team@company.com",
        updater_email="ci-bot@company.com",
        team_name="Analytics",
    )
    assert payload["creator_email"] == "data-team@company.com"
    assert payload["updater_email"] == "ci-bot@company.com"
    assert payload["team_name"] == "Analytics"


def test_map_sync_level_omit_optional_metadata():
    """When creator_email, updater_email, team_name are omitted they are not in payload (API clears)."""
    payload = map_dbt_to_eppo_sync_payload(
        dbt_metrics=SAMPLE_METRICS,
        dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
        sql_map=SAMPLE_SQL_MAP,
        sync_tag="test-run",
    )
    assert "creator_email" not in payload
    assert "updater_email" not in payload
    assert "team_name" not in payload


def test_map_per_metric_override_creator_team():
    """Per-metric eppo_creator_email, eppo_team_name in meta override sync-level for that metric only."""
    metrics_with_override = [
        {
            "name": "total_revenue",
            "label": "Total Revenue",
            "type": "sum",
            "measure": {"name": "revenue"},
            "meta": {"eppo_creator_email": "analyst@company.com", "eppo_team_name": "Growth"},
        },
        {
            "name": "avg_revenue",
            "label": "Avg Revenue",
            "type": "average",
            "measure": {"name": "revenue"},
        },
    ]
    payload = map_dbt_to_eppo_sync_payload(
        dbt_metrics=metrics_with_override,
        dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
        sql_map=SAMPLE_SQL_MAP,
        sync_tag="test-run",
        creator_email="default@company.com",
        team_name="Analytics",
    )
    assert payload["creator_email"] == "default@company.com"
    assert payload["team_name"] == "Analytics"
    eppo_metrics = {m["name"]: m for m in payload["metrics"]}
    # Total Revenue has per-metric override
    assert eppo_metrics["Total Revenue"].get("creator_email") == "analyst@company.com"
    assert eppo_metrics["Total Revenue"].get("team_name") == "Growth"
    # Avg Revenue has no override, so no per-metric fields (uses sync-level)
    assert "creator_email" not in eppo_metrics["Avg Revenue"]
    assert "team_name" not in eppo_metrics["Avg Revenue"]


def test_map_missing_sql():
    """Test warning when compiled SQL is missing for a linked SM."""
    # The mapper now logs a warning and continues instead of raising
    payload = map_dbt_to_eppo_sync_payload(
        dbt_metrics=SAMPLE_METRICS,
        dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
        sql_map={}, # Empty SQL map
        sync_tag="test-run"
    )
    # Should skip the semantic model and fail to map metrics
    assert len(payload['fact_sources']) == 0
    assert len(payload['metrics']) == 0

def test_map_missing_timestamp():
    """Test warning when timestamp column cannot be found."""
    sm_no_ts = [{
        'name': 'users', '_model_unique_id': 'model.proj.dim_users',
        'entities': [{'name': 'user', 'type': 'primary', 'expr': 'user_id'}],
        'dimensions': [{'name': 'country', 'type': 'categorical', 'expr': 'country_code'}], # No time dim
        'measures': [{'name': 'user_count', 'agg': 'count_distinct', 'expr': 'user_id'}]
    }]
    # The mapper now logs a warning and continues instead of raising
    payload = map_dbt_to_eppo_sync_payload(
        dbt_metrics=[],
        dbt_semantic_models=sm_no_ts,
        sql_map=SAMPLE_SQL_MAP,
        sync_tag="test-run"
    )
    # Should skip the semantic model
    assert len(payload['fact_sources']) == 0
    assert len(payload['metrics']) == 0

def test_map_missing_measure_link():
    """Test warning when a metric references a measure not found in SMs."""
    bad_metrics = [{
        'name': 'bad_metric', 'label': 'Bad Metric', 'type': 'sum',
        'measure': {'name': 'non_existent_measure'} # This measure isn't in SAMPLE_SEMANTIC_MODELS
    }]
    # The mapper now logs a warning and continues instead of raising
    payload = map_dbt_to_eppo_sync_payload(
        dbt_metrics=bad_metrics,
        dbt_semantic_models=SAMPLE_SEMANTIC_MODELS,
        sql_map=SAMPLE_SQL_MAP,
        sync_tag="test-run"
    )
    # Should create the fact source but fail to map the metric
    assert len(payload['fact_sources']) == 1
    assert len(payload['metrics']) == 0

# TODO: Add more tests:
# - Mapping of optional fields from meta tags (display_style, is_guardrail etc.)
# - Different filter types (not_equals) and unmappable filters
# - Edge cases: No metrics, no semantic models (should still produce valid empty lists)
# - Complex ratio metrics with different denominator measures
# - Measures without 'expr' (potential row counts) - requires mapper adjustment
