# Visual Layer SDK

A Python SDK for interacting with the Visual Layer API, providing easy access to dataset management, creation, and analysis capabilities.

## Features

- 🔐 **JWT Authentication** - Secure API access with automatic token generation
- 📊 **Dataset Management** - Create, explore, and manage datasets
- 🗂️ **Multiple Data Sources** - Support for S3 buckets and local files
- 📈 **Data Analysis** - Get statistics and explore dataset contents
- 🚀 **Easy Integration** - Simple, intuitive API design
- 📋 **Pandas Integration** - Native DataFrame support for data analysis

## Installation

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

## Quick Start

```python
from visual_layer_sdk import VisualLayerClient

# Initialize client
client = VisualLayerClient(api_key="your_api_key", api_secret="your_api_secret")

# Check API health
health = client.healthcheck()
print(f"API Status: {health}")

# Get all datasets
datasets = client.get_all_datasets()
print(f"Found {len(datasets)} datasets")

# Get a specific dataset
dataset = client.get_dataset_object("your_dataset_id")
stats = dataset.get_stats()
print(f"Dataset stats: {stats}")
```

## API Reference

### VisualLayerClient

The main client class for interacting with the Visual Layer API.

#### Initialization

```python
client = VisualLayerClient(api_key: str, api_secret: str)
```

**Parameters:**
- `api_key` (str): Your Visual Layer API key
- `api_secret` (str): Your Visual Layer API secret

#### Core Methods

##### `healthcheck() -> dict`
Check the health of the Visual Layer API.

```python
health = client.healthcheck()
```

##### `get_sample_datasets() -> list`
Get sample datasets available for testing.

```python
sample_datasets = client.get_sample_datasets()
```

##### `get_all_datasets() -> list`
Retrieve all datasets accessible to your account.

```python
all_datasets = client.get_all_datasets()
```

##### `get_dataset(dataset_id: str) -> pd.DataFrame`
Get dataset details as a pandas DataFrame.

```python
dataset_df = client.get_dataset("your_dataset_id")
```

**Returns:** DataFrame with dataset information including:
- id, created_by, owned_by, display_name
- description, preview_uri, source_type, source_uri
- created_at, updated_at, filename, sample, status, n_images

##### `get_dataset_object(dataset_id: str) -> Dataset`
Get a Dataset object for advanced operations.

```python
dataset = client.get_dataset_object("your_dataset_id")
```

#### Dataset Creation

##### `create_dataset_from_s3_bucket(s3_bucket_path: str, dataset_name: str, pipeline_type: str = None) -> dict`
Create a dataset from files stored in an S3 bucket.
####  `make sure to not include s3:// in the front it is already added`
```python
result = client.create_dataset_from_s3_bucket(
    s3_bucket_path="my-bucket/images/",
    dataset_name="My Image Dataset",
    pipeline_type="image_processing"
)
```

**Parameters:**
- `s3_bucket_path` (str): S3 bucket path containing the files
- `dataset_name` (str): Name for the new dataset
- `pipeline_type` (str, optional): Processing pipeline type

##### `create_dataset_from_local_folder(file_path: str, filename: str, dataset_name: str, pipeline_type: str = None) -> dict`
Create a dataset from a local zip file.

```python
result = client.create_dataset_from_local_folder(
    file_path="/path/to/images.zip",
    filename="images.zip",
    dataset_name="Local Image Dataset",
    pipeline_type="image_processing"
)
```

**Parameters:**
- `file_path` (str): Full path to the zip file
- `filename` (str): Name of the zip file
- `dataset_name` (str): Name for the new dataset
- `pipeline_type` (str, optional): Processing pipeline type

### Dataset

The Dataset class provides methods for working with individual datasets.

#### Initialization

```python
dataset = Dataset(client: VisualLayerClient, dataset_id: str)
```

#### Core Methods

##### `get_stats() -> dict`
Get comprehensive statistics for the dataset.

```python
stats = dataset.get_stats()
```

##### `get_details() -> dict`
Get detailed information about the dataset.

```python
details = dataset.get_details()
```

##### `get_status() -> str`
Get the current status of the dataset.

```python
status = dataset.get_status()
# Returns: "READY", "PROCESSING", "ERROR", etc.
```

##### `explore() -> pd.DataFrame`
Explore the dataset and return previews as a DataFrame.

```python
previews_df = dataset.explore()
```

**Returns:** DataFrame containing dataset previews from the first cluster.

##### `export() -> dict`
Export the dataset in JSON format.

```python
export_data = dataset.export()
```

**Note:** Dataset must be in "READY" or "completed" status to export.

##### `export_to_dataframe() -> pd.DataFrame`
Export the dataset and convert media items to a DataFrame.

```python
media_items_df = dataset.export_to_dataframe()
```

**Returns:** DataFrame containing media items (excluding metadata_items).

##### `delete() -> dict`
Delete the dataset permanently.

```python
result = dataset.delete()
```

##### `search_by_labels_to_dataframe(labels: List[str], entity_type: str = "IMAGES") -> pd.DataFrame`
Search the dataset by labels using VQL asynchronously, poll until export is ready, download the results, and return as a DataFrame.

```python
labels = ["cat", "dog"]
df = dataset.search_by_labels_to_dataframe(labels)
```

- `labels` (List[str]): List of labels to search for
- `entity_type` (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")

**Returns:** DataFrame containing the search results, or empty if not ready or no matches found.

##### `search_by_captions_to_dataframe(caption_text: List[str], entity_type: str = "IMAGES") -> pd.DataFrame`
Search the dataset by captions using VQL asynchronously, poll until export is ready, download the results, and return as a DataFrame.

```python
df = dataset.search_by_captions_to_dataframe(["cat", "sitting", "outdoors"])
```

- `caption_text` (List[str]): List of text strings to search in captions (will be combined into one search string)
- `entity_type` (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")

**Returns:** DataFrame containing the search results, or empty if not ready or no matches found.

##### `search_by_issues_to_dataframe(issue_type: str, confidence_min: float = 0.8, confidence_max: float = 1.0) -> pd.DataFrame`
Search the dataset by issues using VQL asynchronously, poll until export is ready, download the results, and return as a DataFrame.

```python
df = dataset.search_by_issues_to_dataframe(issue_type="outliers")
df = dataset.search_by_issues_to_dataframe(issue_type="blur", confidence_min=0.9)
```

- `issue_type` (str): Issue type to search for (e.g., "blur", "dark", "outliers", "mislabels", "duplicates", "bright", "normal", "label_outlier")
- `confidence_min` (float): Minimum confidence threshold (default: 0.8)
- `confidence_max` (float): Maximum confidence threshold (default: 1.0)

**Returns:** DataFrame containing the search results, or empty if not ready or no matches found.

##### `search_by_visual_similarity_to_dataframe(image_path: str, threshold: str = "0", anchor_type: str = "UPLOAD", entity_type: str = "IMAGES") -> pd.DataFrame`
Search the dataset by visual similarity using a local image file as anchor, poll until export is ready, download the results, and return as a DataFrame.

```python
df = dataset.search_by_visual_similarity_to_dataframe(image_path="/path/to/image.jpg")
df = dataset.search_by_visual_similarity_to_dataframe(image_path="/path/to/image.jpg", threshold="0.5")
```

- `image_path` (str): Path to the image file to use as anchor
- `threshold` (str): Similarity threshold as string (default: "0")
- `anchor_type` (str): Anchor type (default: "UPLOAD")
- `entity_type` (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")

**Returns:** DataFrame containing the search results, or empty if not ready or no matches found.

##### `search_by_vql(vql: List[dict], entity_type: str = "IMAGES") -> pd.DataFrame`
Search the dataset using custom VQL (Visual Query Language) asynchronously, poll until export is ready, download the results, and return as a DataFrame.

```python
# Label search
vql = [{"id": "label_filter", "labels": {"op": "one_of", "value": ["cat", "dog"]}}]
df = dataset.search_by_vql(vql)

# Caption search
vql = [{"text": {"op": "fts", "value": "cat sitting"}}]
df = dataset.search_by_vql(vql)

# Issue search
vql = [{"issues": {"op": "issue", "value": "outliers", "confidence_min": 0.8, "confidence_max": 1.0, "mode": "in"}}]
df = dataset.search_by_vql(vql)

# Visual similarity search
vql = [{"id": "similarity_search", "similarity": {"op": "upload", "value": "media_id"}}]
df = dataset.search_by_vql(vql)
```

- `vql` (List[dict]): VQL query structure as a list of filter objects
- `entity_type` (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")

**Returns:** DataFrame containing the search results, or empty if not ready or no matches found.

##### `search_by_image_file(image_path: str, allow_deleted: bool = False) -> dict`
Upload an image file and get the anchor media ID for similarity search.

```python
result = dataset.search_by_image_file("/path/to/image.jpg")
media_id = result["anchor_media_id"]
anchor_type = result["anchor_type"]
```

- `image_path` (str): Path to the image file (JPEG, PNG, etc.)
- `allow_deleted` (bool): Whether to include deleted images in search (default: False)

**Returns:** Dictionary with `anchor_media_id` and `anchor_type`.

## Complete Example

```python
import os
from dotenv import load_dotenv
from visual_layer_sdk import VisualLayerClient

# Load environment variables
load_dotenv()

# Initialize client
client = VisualLayerClient(
    api_key=os.getenv("VISUAL_LAYER_API_KEY"),
    api_secret=os.getenv("VISUAL_LAYER_API_SECRET")
)

# Check API health
health = client.healthcheck()
print(f"API Health: {health}")

# Create a dataset from S3
result = client.create_dataset_from_s3_bucket(
    s3_bucket_path="s3://my-bucket/images/",
    dataset_name="My Dataset"
)
dataset_id = result["id"]
print(f"Created dataset: {dataset_id}")

# Get dataset object for operations
dataset = client.get_dataset_object(dataset_id)

# Wait for processing to complete
import time
while dataset.get_status() not in ["READY", "completed"]:
    print(f"Dataset status: {dataset.get_status()}")
    time.sleep(30)

# Get dataset statistics
stats = dataset.get_stats()
print(f"Dataset stats: {stats}")

# Explore dataset
previews = dataset.explore()
print(f"Found {len(previews)} previews")

# Export to DataFrame
media_items = dataset.export_to_dataframe()
print(f"Exported {len(media_items)} media items")

# Save to CSV
media_items.to_csv("exported_data.csv", index=False)
```

## Error Handling

The SDK provides comprehensive error handling:

```python
from visual_layer_sdk import VisualLayerException

try:
    dataset = client.get_dataset_object("invalid_id")
    stats = dataset.get_stats()
except VisualLayerException as e:
    print(f"API Error: {e}")
except requests.exceptions.RequestException as e:
    print(f"Network Error: {e}")
except ValueError as e:
    print(f"Validation Error: {e}")
```

## Requirements

- Python 3.8+
- requests
- python-dotenv
- PyJWT
- pandas

## Development

### Running Tests
```bash
pytest tests/ --cov=src/visual_layer_sdk
```

### Code Formatting
```bash
black src/ tests/
ruff check --fix src/ tests/
```

### Installing in Development Mode
```bash
pip install -e ".[dev]"
```

## License

MIT License - see LICENSE file for details.

## Support

For support and questions, please refer to the Visual Layer documentation or contact the development team. 