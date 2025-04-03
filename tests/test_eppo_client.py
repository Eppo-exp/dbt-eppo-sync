# tests/test_client.py

import pytest
import requests
import json
from unittest.mock import MagicMock # Or use pytest-mock's mocker fixture

from dbt_eppo_sync.eppo_client import EppoClient, EppoClientError, METRICS_SYNC_ENDPOINT, DEFAULT_EPPO_API_URL

# --- Constants ---
API_KEY = "test-api-key"
BASE_URL = "https://mock.eppo.cloud/api" # Use a mock base URL
SYNC_ENDPOINT_PATH = METRICS_SYNC_ENDPOINT.lstrip('/') # Path without leading slash

# --- Fixtures ---

@pytest.fixture
def mock_session(mocker):
    """Fixture to mock requests.Session."""
    mock = mocker.patch('requests.Session', autospec=True)
    # Mock the request method on the session instance
    mock_instance = mock.return_value
    mock_instance.request = MagicMock()
    return mock_instance # Return the mocked session instance

@pytest.fixture
def client(mock_session):
    """Fixture to create an EppoClient instance with a mocked session."""
    # mock_session fixture is automatically used here by pytest
    return EppoClient(api_key=API_KEY, base_url=BASE_URL)

# --- Test Cases ---

def test_client_initialization():
    """Test client initialization and header setup."""
    client = EppoClient(api_key=API_KEY, base_url=BASE_URL)
    assert client.base_url == BASE_URL # Should not have trailing slash
    assert client.api_key == API_KEY
    # Check if headers are set correctly on the session
    expected_headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    client.session.headers.update.assert_called_once_with(expected_headers)

def test_client_initialization_missing_key():
    """Test ValueError if API key is missing."""
    with pytest.raises(ValueError, match="Eppo API key is required"):
        EppoClient(api_key="")

def test_sync_definitions_success_json_response(client: EppoClient, mock_session: MagicMock):
    """Test successful sync call with a JSON response."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "success", "message": "Sync processed"}
    mock_session.request.return_value = mock_response

    payload = {"sync_tag": "test", "fact_sources": [], "metrics": []}
    response = client.sync_definitions(payload)

    # Assert request call arguments
    expected_url = f"{BASE_URL}/{SYNC_ENDPOINT_PATH}"
    mock_session.request.assert_called_once_with(
        method="POST",
        url=expected_url,
        params=None,
        json=payload
    )
    # Assert response handling
    mock_response.raise_for_status.assert_called_once()
    assert response == {"status": "success", "message": "Sync processed"}

def test_sync_definitions_success_no_content_response(client: EppoClient, mock_session: MagicMock):
    """Test successful sync call with a 204 No Content response."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 204
    mock_response.content = b'' # No content
    mock_session.request.return_value = mock_response

    payload = {"sync_tag": "test", "fact_sources": [], "metrics": []}
    response = client.sync_definitions(payload)

    expected_url = f"{BASE_URL}/{SYNC_ENDPOINT_PATH}"
    mock_session.request.assert_called_once_with(method="POST", url=expected_url, json=payload, params=None)
    mock_response.raise_for_status.assert_called_once()
    assert response == {} # Expect empty dict for 204

def test_sync_definitions_api_error(client: EppoClient, mock_session: MagicMock):
    """Test handling of Eppo API error (e.g., 4xx/5xx)."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 400
    mock_response.text = '{"error": "Invalid payload", "details": "..."}'
    # Configure the mock to raise HTTPError when raise_for_status is called
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    mock_session.request.return_value = mock_response

    payload = {"sync_tag": "bad-payload"}
    with pytest.raises(EppoClientError) as excinfo:
        client.sync_definitions(payload)

    assert excinfo.value.status_code == 400
    assert "Invalid payload" in excinfo.value.response_text
    assert "Eppo API request failed" in str(excinfo.value)

def test_sync_definitions_network_error(client: EppoClient, mock_session: MagicMock):
    """Test handling of network errors during the request."""
    network_error = requests.exceptions.ConnectionError("Could not connect")
    mock_session.request.side_effect = network_error

    payload = {"sync_tag": "test"}
    with pytest.raises(EppoClientError, match="Network error contacting Eppo API"):
        client.sync_definitions(payload)

def test_sync_definitions_invalid_json_response(client: EppoClient, mock_session: MagicMock):
    """Test handling when API returns non-JSON response on success status."""
    mock_response = MagicMock(spec=requests.Response)
    mock_response.status_code = 200
    mock_response.content = b'This is not JSON'
    mock_response.text = 'This is not JSON'
    # Make response.json() raise an error
    mock_response.json.side_effect = json.JSONDecodeError("Expecting value", "doc", 0)
    mock_session.request.return_value = mock_response

    payload = {"sync_tag": "test"}
    with pytest.raises(EppoClientError, match="Failed to decode JSON response"):
        client.sync_definitions(payload)

# TODO: Add tests for different base URLs, trailing slashes, etc.
