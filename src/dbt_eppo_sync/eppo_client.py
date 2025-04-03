# dbt_eppo_sync/eppo_client.py

import requests
import json
from typing import List, Dict, Any, Optional
import sys

DEFAULT_EPPO_API_URL = "https://eppo.cloud"
METRICS_SYNC_ENDPOINT = "/api/v1/metrics/sync"

class EppoClientError(Exception):
    """Custom exception for Eppo API client errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

    def __str__(self):
        details = f"Status Code: {self.status_code}" if self.status_code else "N/A"
        if self.response_text:
            # Limit response length in error message for readability
            details += f"\nResponse: {self.response_text[:500]}"
        return f"{super().__str__()} ({details})"


class EppoClient:
    """
    A client for interacting with the Eppo API, focusing on the bulk metrics sync endpoint.
    Requires an Eppo API key for authentication.
    """
    def __init__(self, api_key: str, base_url: str = DEFAULT_EPPO_API_URL):
        """
        Initializes the EppoClient.

        Args:
            api_key: Your Eppo API key.
            base_url: The base URL for the Eppo API. Defaults to DEFAULT_EPPO_API_URL.
        """
        if not api_key:
            raise ValueError("Eppo API key is required.")

        self.base_url = base_url.rstrip('/') # Remove trailing slash if present
        self.api_key = api_key
        self.session = requests.Session()
        # Set common headers for all requests
        
        self.session.headers.update({
            'X-Eppo-Token': self.api_key,
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Internal helper method to make API requests.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE).
            endpoint: API endpoint path (e.g., '/api/v1/metrics/sync').
            params: URL parameters for GET requests.
            data: JSON payload for POST/PUT requests.

        Returns:
            The JSON response from the API as a dictionary.

        Raises:
            EppoClientError: If the request fails or returns an error status code.
        """
        # Ensure endpoint starts with a slash
        if not endpoint.startswith('/'):
            endpoint = '/' + endpoint

        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(
                method=method.upper(), # Ensure method is uppercase
                url=url,
                params=params,
                json=data # requests library handles JSON serialization
            )
            # Check for HTTP errors (4xx or 5xx)
            response.raise_for_status()
            # Handle cases where response might be empty (e.g., 204 No Content on success)
            if response.status_code == 204 or not response.content:
                return {} # Return empty dict for successful no-content responses
            # Assume successful responses with content return JSON
            return response.json()

        except requests.exceptions.RequestException as e:
            # Handle connection errors, timeouts, etc.
            # Also try to extract response details if available, as some HTTP errors
            # might be caught here depending on circumstances.
            status_code = None
            response_text = None
            if e.response is not None:
                status_code = e.response.status_code
                response_text = e.response.text
                print(f"ERROR DETAILS FROM EPPO API (HTTP {status_code} - Caught by RequestException):\n---\n{response_text}\n---", file=sys.stderr)

            raise EppoClientError(
                f"Network error contacting Eppo API at {url}: {e}",
                status_code=status_code,
                response_text=response_text
            ) from e
        except requests.exceptions.HTTPError as e:
            # Handle HTTP error status codes from Eppo
            # Print the raw response text for detailed debugging
            print(f"ERROR DETAILS FROM EPPO API (HTTP {e.response.status_code}):\n---\n{e.response.text}\n---", file=sys.stderr)
            raise EppoClientError(
                f"Eppo API request failed for {method.upper()} {url}",
                status_code=e.response.status_code,
                response_text=e.response.text
            ) from e
        except json.JSONDecodeError as e:
             # Handle cases where response is not valid JSON
            raise EppoClientError(
                f"Failed to decode JSON response from Eppo API for {method.upper()} {url}",
                response_text=response.text # Include raw text for debugging
            ) from e

    # --- Bulk Sync Method ---

    def sync_definitions(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends the bulk payload to the Eppo metrics sync endpoint.

        Args:
            payload: A dictionary representing the full sync payload,
                     matching the Eppo '/api/v1/metrics/sync' schema,
                     generated by the mapper.

        Returns:
            A dictionary representing the response from the Eppo API
            (structure depends on Eppo's API definition for this endpoint).

        Raises:
            EppoClientError: If the API request fails.
        """
        # Assuming POST method for sending a large payload. VERIFY this.
        method = "POST"
        endpoint = METRICS_SYNC_ENDPOINT # Use the defined constant

        print(f"INFO: Sending bulk sync payload to {self.base_url}{endpoint}") # Use logging
        # print(f"DEBUG: Payload Snippet: {str(payload)[:500]}...") # Optional debug logging

        return self._request(method, endpoint, data=payload)



# Example Usage (demonstrates calling the ync method)
if __name__ == '__main__':
    import os
    # TODO: Set the EPPO_API_KEY environment variable for this example to run
    api_key = os.environ.get("EPPO_API_KEY")

    if not api_key:
        print("Error: EPPO_API_KEY environment variable not set.")
        print("Skipping EppoClient example usage.")
    else:
        print("Attempting to initialize EppoClient...")
        try:
            client = EppoClient(api_key=api_key) # Uses default base URL

            # --- Test Bulk Sync (Example - Requires a valid payload) ---
            print("\n--- Testing Bulk Sync Definition ---")
            # IMPORTANT: This is a DUMMY payload structure.
            # A real payload must be generated by the updated mapper.py
            # based on parsed dbt artifacts.
            dummy_sync_payload = {
                "sync_tag": "example-run-from-client",
                "fact_sources": [
                    {
                        "name": "example_fact_source",
                        "sql": "SELECT user_id, event_timestamp, revenue FROM events_table",
                        "timestamp_column": "event_timestamp",
                        "entities": [{"entity_name": "user", "column": "user_id"}],
                        "facts": [{"name": "revenue_fact", "column": "revenue", "description": "User revenue"}],
                        "properties": []
                    }
                ],
                "metrics": [
                    {
                        "name": "Total Revenue",
                        "description": "Sum of revenue",
                        "type": "simple",
                        "entity": "user",
                        "numerator": {"fact_name": "revenue_fact", "operation": "sum"}
                    }
                ]
            }

            # Normally, you would generate the payload using the mapper:
            # >> from parser import parse_dbt_artifacts
            # >> from mapper import map_dbt_to_eppo_sync_payload
            # >> metrics, semantic_models, sql_map = parse_dbt_artifacts(...)
            # >> real_payload = map_dbt_to_eppo_sync_payload(metrics, semantic_models, sql_map)
            # >> response = client.sync_definitions(real_payload)

            # --- Make the API call (Commented out by default to prevent accidental calls) ---
            # print("Attempting to send dummy sync payload (COMMENTED OUT)...")
            # try:
            #     # UNCOMMENT BELOW TO ACTUALLY SEND THE DUMMY PAYLOAD
            #     # sync_response = client.sync_definitions(dummy_sync_payload)
            #     # print("Successfully sent sync payload. Response:")
            #     # print(json.dumps(sync_response, indent=2))
            #     print("Skipping actual API call in example.") # Keep commented out
            # except EppoClientError as e:
            #     print(f"Error sending sync payload: {e}")
            # ---------------------------------------------------------------------------------

        except ValueError as e:
            print(f"Initialization Error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            import traceback
            traceback.print_exc()
