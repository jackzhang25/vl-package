# Visual Layer SDK Logging System

The Visual Layer SDK now includes a comprehensive logging system that provides natural, user-friendly output messages for all operations.

## Features

- **Natural Language Messages**: Human-readable output with emojis and clear descriptions
- **Multiple Log Levels**: INFO, DEBUG, WARNING, ERROR, and SUCCESS levels
- **Specialized Methods**: Dedicated logging methods for common operations
- **Configurable Verbosity**: Easy to enable/disable detailed logging
- **Multiple Output Destinations**: Console, file, stderr, or combinations
- **Consistent Formatting**: Uniform message format across all SDK operations

## Where Does Logging Output Go?

By default, all logging output goes to **stdout** (the console/terminal). However, you can configure the logger to output to different destinations:

### Default Behavior (Console Only)
```python
from visual_layer_sdk.client import VisualLayerClient

# Default: logs to console only
client = VisualLayerClient(API_KEY, API_SECRET)
client.logger.info("This appears in the console")
```

### Available Output Destinations

1. **Console (stdout)** - User-friendly messages for immediate feedback
2. **File** - Persistent logging with timestamps for debugging
3. **Error Stream (stderr)** - For error messages and warnings
4. **Multiple Destinations** - Combine any of the above

## Basic Usage

### Import and Initialize

```python
from visual_layer_sdk.client import VisualLayerClient
from visual_layer_sdk.logger import get_logger, set_verbose

# Initialize client (logger is automatically created)
client = VisualLayerClient(API_KEY, API_SECRET)

# Get logger instance
logger = get_logger()
```

### Log Levels

```python
# Info messages (default)
logger.info("Starting dataset creation...")

# Success messages (with ✅ emoji)
logger.success("Dataset created successfully!")

# Warning messages (with ⚠️ emoji)
logger.warning("Dataset is not ready for export")

# Error messages (with ❌ emoji)
logger.error("Failed to connect to API")

# Debug messages (only shown in verbose mode)
logger.debug("Making API request to /dataset/123")
```

## Configuring Logging Output

### Console Only (Default)
```python
from visual_layer_sdk.logger import log_to_console_only

log_to_console_only()
logger = get_logger()
logger.info("This goes to console only")
```

### File Only
```python
from visual_layer_sdk.logger import log_to_file_only

log_to_file_only("my_app.log")
logger = get_logger()
logger.info("This goes to file only")
```

### Console and File
```python
from visual_layer_sdk.logger import log_to_console_and_file

log_to_console_and_file("my_app.log")
logger = get_logger()
logger.info("This goes to both console and file")
```

### Error Stream
```python
from visual_layer_sdk.logger import log_to_stderr

log_to_stderr()
logger = get_logger()
logger.error("This goes to stderr")
```

### Custom Configuration
```python
from visual_layer_sdk.logger import configure_logging
import logging

configure_logging(
    output_destinations=["stdout", "file"],
    log_file="custom.log",
    level=logging.DEBUG
)
logger = get_logger()
```

## Specialized Logging Methods

### Dataset Operations

```python
# Dataset creation
logger.dataset_created("dataset-123", "My Dataset")

# Upload operations
logger.dataset_uploading("My Dataset")
logger.dataset_uploaded("My Dataset")

# Processing status
logger.dataset_processing("My Dataset")
logger.dataset_ready("My Dataset")
logger.dataset_not_ready("dataset-123", "processing")

# Export operations
logger.export_started("dataset-123")
logger.export_completed("dataset-123", 150)
logger.export_failed("dataset-123", "Dataset not found")
```

### Search Operations

```python
# Search operations
logger.search_started("labels", "cat")
logger.search_completed(42, "labels", "cat")

logger.search_started("captions", "people")
logger.search_completed(0, "captions", "people")
```

### API Operations

```python
# Health checks
logger.api_health_check({"status": "healthy"})

# Request details (debug level)
logger.request_details("https://api.example.com/datasets", "GET")
logger.request_success(200)
logger.request_error("Connection timeout")
```

## Verbose Logging

Enable detailed logging to see request details, headers, and response information:

```python
# Enable verbose logging
set_verbose(True)

# Now debug messages will be shown
client.get_sample_datasets()

# Disable verbose logging
set_verbose(False)
```

## Log Level Configuration

```python
from visual_layer_sdk.logger import set_log_level
import logging

# Set specific log level
set_log_level(logging.DEBUG)  # Show all messages
set_log_level(logging.INFO)   # Show info and above
set_log_level(logging.WARNING)  # Show warnings and errors only
set_log_level(logging.ERROR)    # Show errors only
```

## Example Output

### Console Output (Default)
```
✅ API Health Status: {'status': 'healthy'}
📤 Uploading files for dataset 'My Dataset'...
✅ Files uploaded successfully for dataset 'My Dataset'
🔍 Searching for 'cat' using labels...
✅ Found 42 images matching 'cat' using labels
```

### File Output (with timestamps)
```
2024-01-15 10:30:15,123 - INFO - ✅ API Health Status: {'status': 'healthy'}
2024-01-15 10:30:16,456 - INFO - 📤 Uploading files for dataset 'My Dataset'...
2024-01-15 10:30:20,789 - INFO - ✅ Files uploaded successfully for dataset 'My Dataset'
2024-01-15 10:30:21,012 - INFO - 🔍 Searching for 'cat' using labels...
2024-01-15 10:30:22,345 - INFO - ✅ Found 42 images matching 'cat' using labels
```

### Verbose Mode (DEBUG level)
```
GET https://app.visual-layer.com/api/v1/healthcheck
Request successful (Status: 200)
✅ API Health Status: {'status': 'healthy'}
POST https://app.visual-layer.com/api/v1/dataset
Form Data: {'dataset_name': 'My Dataset', ...}
Request successful (Status: 201)
📤 Uploading files for dataset 'My Dataset'...
POST https://app.visual-layer.com/api/v1/dataset/123/upload
File path: /path/to/files.zip
Request successful (Status: 200)
✅ Files uploaded successfully for dataset 'My Dataset'
```

## Integration with Existing Code

The logging system is automatically integrated into all SDK operations. When you use the client methods, you'll see natural output messages:

```python
# This will automatically show progress messages
dataset = client.create_dataset_from_local_folder(
    file_path="/path/to/images.zip",
    filename="images.zip", 
    dataset_name="My Image Dataset"
)

# Output:
# Creating dataset 'My Image Dataset'...
# ✅ Dataset 'My Image Dataset' created successfully (ID: dataset-123)
# 📤 Uploading files for dataset 'My Image Dataset'...
# ✅ Files uploaded successfully for dataset 'My Image Dataset'
```

## Use Cases and Best Practices

### Development Environment
```python
# Console + file for debugging
from visual_layer_sdk.logger import log_to_console_and_file

log_to_console_and_file("development.log")
set_verbose(True)  # Enable debug output
```

### Production Environment
```python
# File only for persistent logs
from visual_layer_sdk.logger import log_to_file_only

log_to_file_only("production.log")
set_verbose(False)  # Disable debug output
```

### Testing Environment
```python
# Console only for immediate feedback
from visual_layer_sdk.logger import log_to_console_only

log_to_console_only()
```

### Error Handling
```python
# Stderr for error messages
from visual_layer_sdk.logger import log_to_stderr

log_to_stderr()
logger = get_logger()
logger.error("Critical error occurred")
```

## Log File Management

### Default Log File Location
If you don't specify a log file path, the logger creates files in a `logs/` directory:
```
logs/visual_layer_sdk_20240115.log
```

### Custom Log File Path
```python
log_to_file_only("/var/log/myapp/visual_layer.log")
```

### Getting Current Log File Path
```python
from visual_layer_sdk.logger import get_log_file_path

current_log_file = get_log_file_path()
print(f"Logging to: {current_log_file}")
```

## Best Practices

1. **Use appropriate log levels**: Use INFO for general progress, DEBUG for technical details, WARNING for potential issues, and ERROR for failures.

2. **Enable verbose logging for debugging**: Use `set_verbose(True)` when troubleshooting API issues.

3. **Configure output based on environment**: Use console for development, file for production.

4. **Check log output**: Monitor the log output to understand what operations are being performed.

5. **Handle errors gracefully**: Use try-catch blocks and let the logger handle error reporting.

6. **Use file logging for persistence**: In production, log to files for debugging and audit trails.

## Migration from Print Statements

All existing print statements have been replaced with appropriate logging calls. The new system provides:

- **Better formatting**: Consistent message format with emojis
- **Log level control**: Ability to filter messages by importance
- **Multiple output destinations**: Console, file, stderr, or combinations
- **Specialized methods**: Domain-specific logging for different operations
- **Better debugging**: Detailed request/response information in verbose mode
- **Persistent logging**: File output with timestamps for debugging

The logging system maintains backward compatibility while providing much better user experience and debugging capabilities. 