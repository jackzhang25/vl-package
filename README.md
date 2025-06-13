# Visual Layer SDK

A Python SDK for interacting with the Visual Layer API.

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file with your API credentials:
```
VISUAL_LAYER_API_KEY=your_api_key_here
VISUAL_LAYER_API_SECRET=your_api_secret_here
```

## Usage

```python
from visual_layer_sdk import VisualLayerClient

# Initialize client
client = VisualLayerClient(api_key="your_api_key", api_secret="your_api_secret")

# Get all datasets
datasets = client.get_all_datasets()

# Get dataset stats
stats = client.get_dataset_stats(dataset_id="your_dataset_id")
```

## Available Methods

- `healthcheck()`: Check API health
- `get_all_datasets()`: Get all datasets
- `get_dataset_stats(dataset_id)`: Get statistics for a specific dataset
- `get_dataset_by_id(dataset_id)`: Get a specific dataset
- `get_sample_datasets()`: Get sample datasets

## Authentication

The SDK requires an API key for authentication. You can obtain your API key from the Visual Layer dashboard. The API key should be passed when initializing the client:

```python
client = VisualLayer(api_key="your-api-key-here")
```

## Features

- Create datasets from local folders
- Type-safe API using Pydantic models
- Easy-to-use interface
- Built-in authentication handling
- Default staging environment support

## Requirements

- Python 3.8+
- requests
- pydantic

## Environment

By default, the SDK connects to the staging environment at `https://app.staging-visual-layer.link/api`. If you need to use a different environment, you can specify a custom host URL:

```python
client = VisualLayer(
    api_key="your-api-key-here",
    host="https://custom-environment-url.com/api"
)
``` 