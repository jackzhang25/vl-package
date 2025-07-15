import logging
import sys
import os
from typing import Optional, List, Union
from datetime import datetime
from pathlib import Path


class VisualLayerLogger:
    """Logger for Visual Layer SDK with natural output messages"""

    def __init__(self, name: str = "visual_layer_sdk", level: int = logging.INFO, output_destinations: List[str] = None, log_file: str = None, log_dir: str = None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # Clear existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Default to stdout if no destinations specified
        if output_destinations is None:
            output_destinations = ["stdout"]

        # Add handlers based on destinations
        for destination in output_destinations:
            if destination == "stdout":
                self._add_stdout_handler()
            elif destination == "stderr":
                self._add_stderr_handler()
            elif destination == "file" and log_file:
                self._add_file_handler(log_file)
            elif destination == "file":
                # Use default log file if none specified
                default_log_file = self._get_default_log_file(log_dir)
                self._add_file_handler(default_log_file)

    def _add_stdout_handler(self):
        """Add stdout handler"""
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _add_stderr_handler(self):
        """Add stderr handler"""
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter("%(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _add_file_handler(self, log_file: str):
        """Add file handler"""
        # Create logs directory if it doesn't exist
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def _get_default_log_file(self, log_dir: str = None) -> str:
        """Get default log file path following standard conventions"""
        if log_dir is None:
            # Use standard log directory locations
            if os.name == "nt":  # Windows
                # Windows: %APPDATA%/VisualLayer/logs/
                appdata = os.getenv("APPDATA", "")
                if appdata:
                    log_dir = os.path.join(appdata, "VisualLayer", "logs")
                else:
                    log_dir = "logs"
            else:  # Unix/Linux/macOS
                # Unix: ~/.local/share/visual-layer/logs/ or /var/log/visual-layer/
                home = os.path.expanduser("~")
                log_dir = os.path.join(home, ".local", "share", "visual-layer", "logs")

        # Create timestamp for daily log files
        timestamp = datetime.now().strftime("%Y-%m-%d")
        return os.path.join(log_dir, f"visual_layer_sdk_{timestamp}.log")

    def info(self, message: str):
        """Log info message"""
        self.logger.info(message)

    def warning(self, message: str):
        """Log warning message"""
        self.logger.warning(f"⚠️  {message}")

    def error(self, message: str):
        """Log error message"""
        self.logger.error(f"❌ {message}")

    def success(self, message: str):
        """Log success message"""
        self.logger.info(f"✅ {message}")

    def debug(self, message: str):
        """Log debug message"""
        self.logger.debug(message)

    def dataset_created(self, dataset_id: str, dataset_name: str):
        """Log dataset creation success"""
        self.success(f"Dataset '{dataset_name}' created successfully (ID: {dataset_id})")

    def dataset_uploading(self, dataset_name: str):
        """Log dataset upload start"""
        self.info(f"📤 Uploading files for dataset '{dataset_name}'...")

    def dataset_uploaded(self, dataset_name: str):
        """Log dataset upload completion"""
        self.success(f"Files uploaded successfully for dataset '{dataset_name}'")

    def dataset_processing(self, dataset_name: str):
        """Log dataset processing start"""
        self.info(f"🔄 Processing dataset '{dataset_name}'...")

    def dataset_ready(self, dataset_name: str):
        """Log dataset ready status"""
        self.success(f"Dataset '{dataset_name}' is ready for use")

    def dataset_not_ready(self, dataset_id: str, status: str):
        """Log dataset not ready warning"""
        self.warning(f"Dataset {dataset_id} is not ready (status: {status})")

    def search_started(self, search_type: str, query: str):
        """Log search operation start"""
        self.info(f"🔍 Searching for '{query}' using {search_type}...")

    def search_completed(self, count: int, search_type: str, query: str):
        """Log search operation completion"""
        if count > 0:
            self.success(f"Found {count} images matching '{query}' using {search_type}")
        else:
            self.info(f"No images found matching '{query}' using {search_type}")

    def api_health_check(self, status: dict):
        """Log API health check result"""
        self.success(f"API Health Status: {status}")

    def request_details(self, url: str, method: str = "GET"):
        """Log request details (debug level)"""
        self.debug(f"{method} {url}")

    def request_success(self, status_code: int):
        """Log successful request"""
        self.debug(f"Request successful (Status: {status_code})")

    def request_error(self, error: str):
        """Log request error"""
        self.error(f"Request failed: {error}")

    def export_started(self, dataset_id: str):
        """Log export operation start"""
        self.info(f"📤 Exporting dataset {dataset_id}...")

    def export_completed(self, dataset_id: str, item_count: int):
        """Log export operation completion"""
        self.success(f"Exported {item_count} items from dataset {dataset_id}")

    def export_failed(self, dataset_id: str, error: str):
        """Log export operation failure"""
        self.error(f"Failed to export dataset {dataset_id}: {error}")


# Global logger instance
_logger: Optional[VisualLayerLogger] = None


def get_logger() -> VisualLayerLogger:
    """Get the global logger instance"""
    global _logger
    if _logger is None:
        _logger = VisualLayerLogger()
    return _logger


def set_log_level(level: int):
    """Set the log level for the global logger"""
    global _logger
    if _logger is None:
        _logger = VisualLayerLogger()
    _logger.logger.setLevel(level)


def set_verbose(verbose: bool = True):
    """Set verbose logging mode"""
    level = logging.DEBUG if verbose else logging.INFO
    set_log_level(level)


def configure_logging(output_destinations: List[str] = None, log_file: str = None, level: int = logging.INFO, log_dir: str = None):
    """
    Configure logging output destinations and settings.

    Args:
        output_destinations: List of destinations ("stdout", "stderr", "file")
        log_file: Path to log file (required if "file" is in destinations)
        level: Logging level
        log_dir: Directory for log files (if not using default)
    """
    global _logger
    _logger = VisualLayerLogger(level=level, output_destinations=output_destinations, log_file=log_file, log_dir=log_dir)


def log_to_console_only():
    """Configure logging to output only to console (stdout)"""
    configure_logging(output_destinations=["stdout"])


def log_to_file_only(log_file: str = None, log_dir: str = None):
    """Configure logging to output only to file"""
    configure_logging(output_destinations=["file"], log_file=log_file, log_dir=log_dir)


def log_to_console_and_file(log_file: str = None, log_dir: str = None):
    """Configure logging to output to both console and file"""
    configure_logging(output_destinations=["stdout", "file"], log_file=log_file, log_dir=log_dir)


def log_to_stderr():
    """Configure logging to output to stderr (for errors)"""
    configure_logging(output_destinations=["stderr"])


def get_log_file_path() -> str:
    """Get the current log file path if logging to file is enabled"""
    global _logger
    if _logger is None:
        return ""

    for handler in _logger.logger.handlers:
        if isinstance(handler, logging.FileHandler):
            return handler.baseFilename
    return ""


def get_default_log_directory() -> str:
    """Get the default log directory path"""
    if os.name == "nt":  # Windows
        appdata = os.getenv("APPDATA", "")
        if appdata:
            return os.path.join(appdata, "VisualLayer", "logs")
        else:
            return "logs"
    else:  # Unix/Linux/macOS
        home = os.path.expanduser("~")
        return os.path.join(home, ".local", "share", "visual-layer", "logs")


def list_log_files(log_dir: str = None) -> List[str]:
    """List all log files in the log directory"""
    if log_dir is None:
        log_dir = get_default_log_directory()

    if not os.path.exists(log_dir):
        return []

    log_files = []
    for file in os.listdir(log_dir):
        if file.startswith("visual_layer_sdk_") and file.endswith(".log"):
            log_files.append(os.path.join(log_dir, file))

    return sorted(log_files)


def get_latest_log_file(log_dir: str = None) -> str:
    """Get the path to the most recent log file"""
    log_files = list_log_files(log_dir)
    if log_files:
        return log_files[-1]  # Last file is most recent
    return ""


def show_log_directory_info():
    """Show information about the log directory and files"""
    log_dir = get_default_log_directory()
    print(f"📁 Default log directory: {log_dir}")

    if os.path.exists(log_dir):
        log_files = list_log_files(log_dir)
        print(f"📄 Found {len(log_files)} log files:")
        for log_file in log_files[-5:]:  # Show last 5 files
            file_size = os.path.getsize(log_file)
            file_time = datetime.fromtimestamp(os.path.getmtime(log_file))
            print(f"  • {os.path.basename(log_file)} ({file_size} bytes, {file_time.strftime('%Y-%m-%d %H:%M')})")
    else:
        print("📄 No log directory found yet")

    current_log_file = get_log_file_path()
    if current_log_file:
        print(f"🔄 Currently logging to: {current_log_file}")
    else:
        print("🔄 Currently logging to console only")
