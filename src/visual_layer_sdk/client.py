import json
from typing import Any, Dict, Optional

import requests

from .exceptions import VisualLayerError


class VisualLayerClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://app.staging-visual-layer.link/api/v1",
    ):
        """
        Initialize the Visual Layer API client.

        Args:
            api_key (str): Your Visual Layer API key.
            base_url (str): The base URL for the Visual Layer API.
        """
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {api_key}", "Accept": "application/json"})

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """
        Make an HTTP request to the Visual Layer API.

        Args:
            method (str): HTTP method (GET, POST, etc.)
            endpoint (str): API endpoint path
            **kwargs: Additional arguments to pass to requests

        Returns:
            Dict[str, Any]: JSON response from the API

        Raises:
            VisualLayerError: If the request fails
        """
        url = f"{self.base_url}{endpoint}"
        print("\n=== API Request Details ===")
        print(f"Method: {method}")
        print(f"URL: {url}")
        print(f"Base URL: {self.base_url}")
        print(f"Endpoint: {endpoint}")

        # Handle request body based on content type
        if "json" in kwargs:
            print(f"Request body (JSON): {json.dumps(kwargs['json'], indent=2)}")
            self.session.headers.update({"Content-Type": "application/json"})
        elif "data" in kwargs:
            print(f"Request body (form): {kwargs['data']}")
            self.session.headers.update({"Content-Type": "application/x-www-form-urlencoded"})
            # Convert data to form-urlencoded format
            if isinstance(kwargs["data"], dict):
                kwargs["data"] = requests.compat.urlencode(kwargs["data"])

        print(f"Headers: {json.dumps(dict(self.session.headers), indent=2)}")

        try:
            print("\n=== Making Request ===")
            response = self.session.request(method, url, **kwargs)
            print(f"Response status code: {response.status_code}")
            print(f"Response headers: {json.dumps(dict(response.headers), indent=2)}")
            try:
                print(f"Response body: {json.dumps(response.json(), indent=2)}")
            except (ValueError, json.JSONDecodeError):
                print(f"Response body: {response.text}")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print("\n=== Request Failed ===")
            print(f"Error type: {type(e).__name__}")
            print(f"Error message: {str(e)}")
            if hasattr(e, "response"):
                print(f"Error response status code: {e.response.status_code}")
                print(f"Error response headers: {json.dumps(dict(e.response.headers), indent=2)}")
                print(f"Error response body: {e.response.text}")
            raise VisualLayerError(f"API request failed: {str(e)}")

    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a GET request to the API.

        Args:
            endpoint (str): API endpoint path
            params (Optional[Dict[str, Any]]): Query parameters

        Returns:
            Dict[str, Any]: JSON response from the API
        """
        return self._make_request("GET", endpoint, params=params)

    def post(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make a POST request to the API.

        Args:
            endpoint (str): API endpoint path
            params (Optional[Dict[str, Any]]): Query parameters
            json (Optional[Dict[str, Any]]): JSON body
            data (Optional[Dict[str, Any]]): Form data

        Returns:
            Dict[str, Any]: JSON response from the API
        """
        return self._make_request("POST", endpoint, params=params, json=json, data=data)

    def put(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make a PUT request to the API.

        Args:
            endpoint (str): API endpoint path
            params (Optional[Dict[str, Any]]): Query parameters
            json (Optional[Dict[str, Any]]): JSON body
            data (Optional[Dict[str, Any]]): Form data

        Returns:
            Dict[str, Any]: JSON response from the API
        """
        return self._make_request("PUT", endpoint, params=params, json=json, data=data)

    def delete(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a DELETE request to the API.

        Args:
            endpoint (str): API endpoint path
            params (Optional[Dict[str, Any]]): Query parameters

        Returns:
            Dict[str, Any]: JSON response from the API
        """
        return self._make_request("DELETE", endpoint, params=params)
