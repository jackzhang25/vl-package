import pandas as pd
import json
from typing import List
from enum import Enum

from .logger import get_logger


class SearchOperator(Enum):
    IS = "is"
    IS_NOT = "is not"
    IS_ONE_OF = "is one of"
    IS_NOT_ONE_OF = "is not one of"


ISSUE_TYPE_MAPPING = {
    0: {"name": "mislabels", "description": "Mislabeled items", "severity": 0},
    1: {"name": "outliers", "description": "Outlier detection", "severity": 0},
    2: {"name": "duplicates", "description": "Duplicate detection", "severity": 0},
    3: {"name": "blur", "description": "Blurry images", "severity": 1},
    4: {"name": "dark", "description": "Dark images", "severity": 1},
    5: {"name": "bright", "description": "Bright images", "severity": 2},
    6: {"name": "normal", "description": "Normal images", "severity": 0},
    7: {"name": "label_outlier", "description": "Label outliers", "severity": 0},
}
ALLOWED_ISSUE_NAMES = {v["name"] for v in ISSUE_TYPE_MAPPING.values()}


class Dataset:
    # TODO: add id and name fields
    def __init__(self, client, dataset_id: str, poll_interval: int = 10, timeout: int = 300):
        self.client = client
        self.dataset_id = dataset_id
        self.base_url = client.base_url
        self.logger = get_logger()
        self.poll_interval = poll_interval
        self.timeout = timeout

        # Validate that the dataset exists
        self._validate_dataset_exists()

    def _validate_dataset_exists(self):
        """Validate that the dataset exists by calling the get_details API"""
        try:
            response = self.client.session.get(
                f"{self.base_url}/dataset/{self.dataset_id}",
                headers=self.client._get_headers(),
            )
            response.raise_for_status()
        except Exception as e:
            if "Not Found" in str(e) or response.status_code == 404:
                raise ValueError(f"Dataset '{self.dataset_id}' does not exist. Please check the dataset ID and try again.")
            else:
                raise RuntimeError(f"Failed to validate dataset '{self.dataset_id}': {str(e)}")

    def __str__(self) -> str:
        """String representation of the dataset with its details"""
        try:
            details = self.get_details()
            status = details.get("status", "Unknown")
            display_name = details.get("display_name", "No name")
            description = details.get("description", "No description")
            created_at = details.get("created_at", "Unknown")
            filename = details.get("filename", "me")

            return f"Dataset(id='{self.dataset_id}', name='{display_name}', status='{status}', filename='{filename}', created_at='{created_at}', description={description})"
        except Exception as e:
            return f"Dataset(id='{self.dataset_id}', error='{str(e)}')"

    def get_stats(self) -> dict:
        """Get statistics for this dataset"""
        response = self.client.session.get(
            f"{self.base_url}/dataset/{self.dataset_id}/stats",
            headers=self.client._get_headers(),
        )
        response.raise_for_status()
        return response.json()

    def get_details(self) -> dict:
        """Get details for this dataset"""
        response = self.client.session.get(
            f"{self.base_url}/dataset/{self.dataset_id}",
            headers=self.client._get_headers(),
        )
        response.raise_for_status()
        full_response = response.json()

        # Filter to only include the specified fields
        selected_fields = [
            "id",
            "created_by",
            "source_dataset_id",
            "owned_by",
            "display_name",
            "description",
            "preview_uri",
            "source_type",
            "source_uri",
            "created_at",
            "updated_at",
            "filename",
            "sample",
            "status",
        ]

        # Create filtered dictionary with only the selected fields
        filtered_details = {field: full_response.get(field) for field in selected_fields}

        return filtered_details

    def explore(self) -> pd.DataFrame:
        """Explore this dataset and return previews as a DataFrame"""
        response = self.client.session.get(
            f"{self.base_url}/explore/{self.dataset_id}",
            headers=self.client._get_headers(),
        )
        response.raise_for_status()
        data = response.json()

        # Extract just the previews from the first cluster
        if data.get("clusters") and len(data["clusters"]) > 0:
            previews = data["clusters"][0].get("previews", [])
            # Convert previews to DataFrame
            df = pd.DataFrame(previews)
            return df
        else:
            return pd.DataFrame()  # Return empty DataFrame if no previews found

    def delete(self) -> dict:
        """Delete this dataset"""
        response = self.client.session.delete(
            f"{self.base_url}/dataset/{self.dataset_id}",
            headers=self.client._get_headers(),
        )
        response.raise_for_status()
        return response.json()

    def get_image_info(self, image_id) -> list:
        response = self.client.session.get(
            f"{self.base_url}/image/{image_id}",
            headers=self.client._get_headers(),
        )
        response.raise_for_status()
        return response.json()

    # include image_uri in export
    def export(self) -> dict:
        """Export this dataset in JSON format"""
        # Check if dataset is ready before exporting
        status = self.get_status()
        if status not in ["READY", "completed"]:
            raise RuntimeError(f"Cannot export dataset {self.dataset_id}. Current status: {status}. Dataset must be 'ready' or 'completed' to export.")

        url = f"{self.base_url}/dataset/{self.dataset_id}/export"
        params = {"export_format": "json"}
        headers = {**self.client._get_headers()}
        response = self.client.session.get(url, params=params, headers=headers)
        response.raise_for_status()
        return response.json()

    def export_to_dataframe(self) -> pd.DataFrame:
        """
        Export this dataset and convert media_items to a DataFrame.

        Returns:
            pd.DataFrame: DataFrame containing media_items (excluding metadata_items)
        """
        try:
            # Check if dataset is ready before exporting
            status = self.get_status()
            if status not in ["READY", "completed"]:
                self.logger.dataset_not_ready(self.dataset_id, status)
                return pd.DataFrame()

            # Export the dataset
            export_data = self.export()

            # Extract media_items from the export data
            if "media_items" in export_data:
                media_items = export_data["media_items"]

                # Remove metadata_items from each media item if it exists
                cleaned_media_items = []
                for item in media_items:
                    # Create a copy of the item without metadata_items
                    cleaned_item = {k: v for k, v in item.items() if k != "metadata_items"}
                    cleaned_media_items.append(cleaned_item)

                # Convert to DataFrame
                df = pd.DataFrame(cleaned_media_items)
                self.logger.export_completed(self.dataset_id, len(df))
                return df
            else:
                self.logger.warning("No media_items found in export data")
                return pd.DataFrame()

        except Exception as e:
            self.logger.export_failed(self.dataset_id, str(e))
            return pd.DataFrame()

    def get_status(self) -> dict:
        return self.get_details()["status"]

    def process_export_download_to_dataframe(self, download_uri: str) -> pd.DataFrame:
        """
        Download the export results from the provided URI and flatten to a DataFrame.
        Can be used by any async search function that returns a download_uri.

        Args:
            download_uri (str): The download URI from the export status response

        Returns:
            pd.DataFrame: DataFrame containing the search results, or empty if not valid
        """
        # Download and process the export results
        export_data = self.download_export_results(download_uri)
        if not export_data or "media_items" not in export_data:
            self.logger.warning("No media_items found in downloaded export data")
            return pd.DataFrame()

        # Flatten to DataFrame (reuse export_to_dataframe logic)
        processed_items = []
        for item in export_data["media_items"]:
            processed_item = item.copy()
            metadata_items = item.get("metadata_items", [])
            processed_item["captions"] = []
            processed_item["image_labels"] = []
            processed_item["object_labels"] = []
            processed_item["issues"] = []
            for metadata in metadata_items:
                metadata_type = metadata.get("type")
                properties = metadata.get("properties", {})
                if metadata_type == "caption":
                    caption = properties.get("caption", "")
                    if caption:
                        processed_item["captions"].append(caption)
                elif metadata_type == "image_label":
                    category = properties.get("category_name", "")
                    source = properties.get("source", "")
                    if category:
                        processed_item["image_labels"].append(f"{category}({source})")
                elif metadata_type == "object_label":
                    category = properties.get("category_name", "")
                    bbox = properties.get("bbox", [])
                    if category:
                        processed_item["object_labels"].append(f"{category}{bbox}")
                elif metadata_type == "issue":
                    issue_type = properties.get("issue_type", "")
                    description = properties.get("issues_description", "")
                    confidence = properties.get("confidence", 0.0)
                    if issue_type:
                        processed_item["issues"].append(f"{issue_type}:{description}({confidence:.3f})")
            processed_item["captions"] = "; ".join(processed_item["captions"])
            processed_item["image_labels"] = "; ".join(processed_item["image_labels"])
            processed_item["object_labels"] = "; ".join(processed_item["object_labels"])
            processed_item["issues"] = "; ".join(processed_item["issues"])
            processed_item.pop("metadata_items", None)
            processed_items.append(processed_item)

        df = pd.DataFrame(processed_items)
        self.logger.export_completed(self.dataset_id, len(df))
        return df

    def search_by_visual_similarity_to_dataframe(self, image_path: str, threshold: str = "0", anchor_type: str = "UPLOAD", entity_type: str = "IMAGES") -> pd.DataFrame:
        """
        Search dataset by visual similarity asynchronously, poll until export is ready, download the results, and return as a DataFrame.
        Args:
            image_path (str): Path to the image file to use as anchor
            threshold (str): Similarity threshold as string (default: "0")
            anchor_type (str): Anchor type (default: 'UPLOAD')
            entity_type (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")
        Returns:
            pd.DataFrame: DataFrame containing the search results, or empty if not ready
        Raises:
            ValueError: If image_path is not provided
        """
        import time

        start_time = time.time()
        # Get media_id from image file upload
        upload_result = self.search_by_image_file(image_path=image_path)
        media_id = upload_result.get("anchor_media_id")
        if not media_id:
            raise ValueError("Failed to get anchor_media_id from image upload")

        # Form the VQL for visual similarity search
        vql = [{"id": "similarity_search", "similarity": {"op": "upload", "value": media_id}}]

        # Step 1: Start async search and get initial status using the general VQL function
        return self.search_by_vql(vql, entity_type)

    def search_by_captions_to_dataframe(self, caption_text: List[str], entity_type: str = "IMAGES") -> pd.DataFrame:
        """
        Search dataset by captions using VQL asynchronously, poll until export is ready, download the results, and return as a DataFrame.

        Args:
            caption_text (List[str]): List of text strings to search in captions (will be combined into one search string)
            entity_type (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")

        Returns:
            pd.DataFrame: DataFrame containing the search results, or empty if not ready
        """
        # Combine the list of strings into one search string
        combined_text = " ".join(caption_text)

        # Form the VQL for caption search
        vql = [{"text": {"op": "fts", "value": combined_text}}]

        # Step 1: Start async search and get initial status using the general VQL function
        return self.search_by_vql(vql, entity_type)

    def search_by_labels_to_dataframe(self, labels: List[str], entity_type: str = "IMAGES") -> pd.DataFrame:
        """
        Search dataset by labels using VQL asynchronously, poll until export is ready, download the results, and return as a DataFrame.

        Args:
            labels (List[str]): List of labels to search for
            entity_type (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")

        Returns:
            pd.DataFrame: DataFrame containing the search results, or empty if not ready
        """

        # Form the VQL for label search
        vql = [{"id": "label_filter", "labels": {"op": "one_of", "value": labels}}]

        # Step 1: Start async search and get initial status using the general VQL function
        return self.search_by_vql(vql, entity_type)

    def search_by_issues_to_dataframe(self, issue_type: str = None, confidence_min: float = 0.8, confidence_max: float = 1.0) -> pd.DataFrame:
        """
        Search dataset by issues using VQL and return as DataFrame.

        Args:
            issue_types (List[str]): List of issue types to search for (e.g., ["blur", "dark", "outliers"])
            confidence_min (float): Minimum confidence threshold (default: 0.8)
            confidence_max (float): Maximum confidence threshold (default: 1.0)

        Returns:
            pd.DataFrame: DataFrame containing the search results
        """
        if not issue_type:
            raise ValueError("issue_type must be provided")

        # Validate issue types against allowed names

        if issue_type not in ALLOWED_ISSUE_NAMES:
            raise ValueError(f"Invalid issue type '{issue_type}'. Allowed types: {list(ALLOWED_ISSUE_NAMES)}")

        # Build VQL for issue search
        vql = [{"issues": {"op": "issue", "value": issue_type, "confidence_min": confidence_min, "confidence_max": confidence_max, "mode": "in"}}]

        # Call the general VQL search function
        return self.search_by_vql(vql, "IMAGES")

    def search_by_vql(self, vql: List[dict], entity_type: str = "IMAGES") -> dict:
        """
        Search dataset using custom VQL (Visual Query Language).
        Makes the first API call to export_context_async and returns the response.

        Args:
            vql (List[dict]): VQL query structure as a list of filter objects
            entity_type (str): Entity type to search ("IMAGES" or "OBJECTS", default: "IMAGES")

        Returns:
            dict: Export task response with task ID, status, etc.
        """
        if not vql:
            self.logger.warning("No VQL provided for search")
            return {}

        url = f"{self.base_url}/dataset/{self.dataset_id}/export_context_async"
        params = {"export_format": "json", "include_images": False, "entity_type": entity_type, "vql": json.dumps(vql)}

        try:
            import time

            start_time = time.time()
            self.logger.info(f"Starting VQL search with query: {vql}")
            response = self.client.session.get(url, headers=self.client._get_headers(), params=params)
            response.raise_for_status()
            status_result = response.json()
            self.logger.success(f"VQL search export task created successfully")
            download_uri = status_result.get("download_uri")
            status = status_result.get("status")
            export_task_id = status_result.get("id")

            # If no download_uri in the first response, return empty DataFrame immediately
            if status == "REJECTED" or status is None:
                self.logger.info("No images matched the VQL label search. Returning empty DataFrame.")
                return pd.DataFrame()

            # Poll if not ready
            while (status != "COMPLETED" or not download_uri) and (time.time() - start_time < self.timeout):
                self.logger.info(f"Export not ready (status: {status}). Waiting {self.poll_interval}s before polling again...")
                time.sleep(self.poll_interval)
                # Poll status endpoint
                poll_status = self.client.session.get(
                    f"{self.client.base_url}/dataset/{self.dataset_id}/export_status",
                    headers=self.client._get_headers(),
                    params={"export_task_id": export_task_id, "dataset_id": self.dataset_id},
                )
                poll_status.raise_for_status()
                status_result = poll_status.json()
                download_uri = status_result.get("download_uri")
                status = status_result.get("status")

                # Check if export was rejected during polling
                if status == "REJECTED":
                    result_message = status_result.get("result_message", "No reason provided")
                    self.logger.error(f"Export request rejected during polling: {result_message}")
                    print(f"❌ Export request rejected during polling: {result_message}")
                    return pd.DataFrame()

            if status != "COMPLETED" or not download_uri:
                self.logger.warning(f"Export not completed or no download_uri after waiting. Final status: {status}")
                return pd.DataFrame()

            # Step 2: Use the general processor
            return self.process_export_download_to_dataframe(download_uri)
        except Exception as e:
            self.logger.error(f"VQL search failed: {str(e)}")
            raise

    # 4 booleans to see if each search is available
    def download_export_results(self, download_uri: str) -> dict:
        """
        Download the export results from the provided URI.
        Handles both ZIP (with JSON inside) and direct JSON responses.

        Args:
            download_uri (str): The download URI from the export status response

        Returns:
            dict: The downloaded export data (parsed from JSON inside ZIP if needed)
        """
        import io
        import zipfile

        try:
            self.logger.info(f"Downloading export results from: {download_uri}")
            response = self.client.session.get(download_uri)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            self.logger.debug(f"Response content type: {content_type}")
            self.logger.debug(f"Response status code: {response.status_code}")
            self.logger.debug(f"Response size: {len(response.content)} bytes")

            # Try ZIP extraction first (since we know it works)
            self.logger.info("Attempting ZIP extraction...")
            try:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    self.logger.debug(f"ZIP contents: {zf.namelist()}")

                    # Look specifically for metadata.json
                    if "metadata.json" in zf.namelist():
                        self.logger.info("Found metadata.json")
                        with zf.open("metadata.json") as json_file:
                            json_bytes = json_file.read()
                            json_str = json_bytes.decode("utf-8")
                            result = json.loads(json_str)
                            self.logger.info(f"Successfully extracted and parsed JSON")
                            self.logger.debug(f"JSON keys: {list(result.keys())}")
                            if "media_items" in result:
                                self.logger.info(f"Number of media items: {len(result['media_items'])}")
                            self.logger.success(f"Export results downloaded and extracted from ZIP successfully")
                            return result
                    else:
                        self.logger.warning("metadata.json not found")
                        self.logger.debug(f"Available files: {zf.namelist()}")
                        raise ValueError("metadata.json not found in ZIP archive.")

            except Exception as zip_error:
                self.logger.warning(f"ZIP extraction failed: {str(zip_error)}")
                # Fall through to try JSON parsing

            # If ZIP extraction failed, try JSON parsing
            if "application/json" in content_type:
                result = response.json()
                self.logger.debug(f"Parsed as JSON")
                self.logger.debug(f"Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
                self.logger.success(f"Export results downloaded successfully")
                return result
            else:
                # Handle non-JSON responses (like text)
                self.logger.debug(f"Content type: {content_type}")
                self.logger.debug(f"Response size: {len(response.content)} bytes")
                # Try to parse as JSON anyway (in case content-type is wrong)
                try:
                    result = response.json()
                    self.logger.info("Successfully parsed as JSON")
                    self.logger.debug(f"Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
                except Exception as json_error:
                    self.logger.warning(f"Failed to parse as JSON: {str(json_error)}")
                    result = {"content_type": content_type, "size_bytes": len(response.content), "raw_text": response.text[:1000], "error": "Response is not valid JSON"}
                self.logger.success(f"Export results downloaded (non-JSON fallback)")
                return result

        except Exception as e:
            self.logger.error(f"Export download failed: {str(e)}")
            raise

    def search_by_image_file(self, image_path: str, allow_deleted: bool = False) -> dict:
        """
        Search for visually similar images using a local image file.
        Uploads the image and searches for similar images in the dataset.

        Args:
            image_path (str): Path to the image file (JPEG, PNG, etc.)
            allow_deleted (bool): Whether to include deleted images in search (default: False)

        Returns:
            dict: API response with anchor_media_id and anchor_type
        """
        import os
        from pathlib import Path

        # Validate file exists
        if not os.path.exists(image_path):
            raise ValueError(f"Image file not found: {image_path}")

        # Get file info
        file_path = Path(image_path)
        if not file_path.is_file():
            raise ValueError(f"Path is not a file: {image_path}")

        # Determine content type
        content_type = "image/jpeg"  # Default
        if file_path.suffix.lower() in [".png"]:
            content_type = "image/png"
        elif file_path.suffix.lower() in [".jpg", ".jpeg"]:
            content_type = "image/jpeg"

        url = f"{self.base_url}/dataset/{self.dataset_id}/search-image-similarity"
        params = {"allow_deleted": allow_deleted}

        try:
            self.logger.info(f"Uploading image file for similarity search: {image_path}")

            # Prepare multipart form data
            with open(image_path, "rb") as file:
                files = {"file": (file_path.name, file, content_type)}

                # Remove Content-Type header to let requests set it for multipart
                headers = self.client._get_headers()
                headers.pop("Content-Type", None)

                self.logger.debug(f"URL: {url}")
                self.logger.debug(f"Params: {params}")
                self.logger.debug(f"Headers: {headers}")
                self.logger.debug(f"File: {file_path.name}, Content-Type: {content_type}")

                response = self.client.session.post(url, headers=headers, params=params, files=files)

                self.logger.debug(f"Response status: {response.status_code}")
                self.logger.debug(f"Response headers: {dict(response.headers)}")

                if response.status_code != 200:
                    self.logger.debug(f"Response text: {response.text}")

                response.raise_for_status()
                result = response.json()

                self.logger.success(f"Image similarity search completed successfully")
                return result

        except Exception as e:
            self.logger.error(f"Image similarity search failed: {str(e)}")
            raise
