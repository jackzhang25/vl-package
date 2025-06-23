"""Tests for the VisualLayerClient."""

import pytest
from unittest.mock import Mock, patch
from src.visual_layer_sdk.client import VisualLayerClient


class TestVisualLayerClient:
    """Test cases for VisualLayerClient."""

    def test_client_initialization(self):
        """Test that the client initializes correctly."""
        api_key = "test_key"
        api_secret = "test_secret"

        client = VisualLayerClient(api_key, api_secret)

        assert client.api_key == api_key
        assert client.api_secret == api_secret
        assert client.base_url == "https://app.visual-layer.com/api/v1"

    def test_generate_jwt(self):
        """Test JWT token generation."""
        api_key = "test_key"
        api_secret = "test_secret"

        client = VisualLayerClient(api_key, api_secret)
        jwt_token = client._generate_jwt()

        assert jwt_token is not None
        assert isinstance(jwt_token, str)

    def test_get_headers(self):
        """Test header generation."""
        api_key = "test_key"
        api_secret = "test_secret"

        client = VisualLayerClient(api_key, api_secret)
        headers = client._get_headers()

        assert "Authorization" in headers
        assert headers["accept"] == "application/json"
        assert headers["Content-Type"] == "application/json"
        assert headers["Authorization"].startswith("Bearer ")

    def test_get_headers_no_jwt(self):
        """Test header generation without JWT."""
        api_key = "test_key"
        api_secret = "test_secret"

        client = VisualLayerClient(api_key, api_secret)
        headers = client._get_headers_no_jwt()

        assert "Authorization" not in headers
        assert headers["accept"] == "application/json"
        assert headers["Content-Type"] == "application/json"
