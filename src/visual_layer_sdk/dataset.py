import pandas as pd
import json
from typing import List

from .logger import get_logger


class Dataset:
    # TODO: add id and name fields
    def __init__(self, client, dataset_id: str):
        self.client = client
        self.dataset_id = dataset_id
        self.base_url = client.base_url
        self.logger = get_logger()

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

    def search_by_captions(self, caption_text: str, similarity_threshold: float = 0.83) -> pd.DataFrame:
        """
        Search dataset by AI-generated captions and return all images as a DataFrame.

        Args:
            caption_text (str): Text to search in captions
            similarity_threshold (float): Threshold for semantic search (default: 0.83)

        Returns:
            pd.DataFrame: DataFrame containing all images from matching clusters
        """
        if not caption_text:
            return pd.DataFrame()

        # --- Paginate cluster retrieval ---
        cluster_ids = []
        all_cluster_ids = set()
        page_number = 0
        while True:
            params = {
                "verbose": "false",
                "allow_deleted": "false",
                "caption_only_filter": caption_text,
                "page_number": page_number,
            }
            response = self.client.session.get(
                f"{self.base_url}/explore/{self.dataset_id}",
                params=params,
                headers=self.client._get_headers(),
            )
            response.raise_for_status()
            data = response.json()
            clusters = data.get("clusters", [])
            if not clusters:
                break
            for cluster in clusters:
                cluster_id = cluster.get("cluster_id")
                if cluster_id and cluster_id not in all_cluster_ids:
                    cluster_ids.append(cluster_id)
                    all_cluster_ids.add(cluster_id)
            page_number += 1

        # --- Paginate image retrieval for each cluster ---
        all_images = []
        seen_image_ids = set()  # To prevent duplicates
        for cluster_id in cluster_ids:
            page_number = 0
            while True:
                cluster_params = {
                    "verbose": "true",
                    "allow_deleted": "false",
                    "caption_only_filter": caption_text,
                    "page_number": page_number,
                }
                cluster_response = self.client.session.get(
                    f"{self.base_url}/explore/{self.dataset_id}/similarity_cluster/{cluster_id}",
                    params=cluster_params,
                    headers=self.client._get_headers(),
                )
                cluster_response.raise_for_status()
                cluster_data = cluster_response.json()
                if cluster_data is None:
                    break
                previews = cluster_data.get("previews", [])
                if not previews:
                    break
                for preview in previews:
                    image_id = preview.get("image_id") or preview.get("id")
                    if image_id and image_id in seen_image_ids:
                        continue
                    if image_id:
                        seen_image_ids.add(image_id)
                    image_data = preview.copy()
                    if "labels" in image_data and isinstance(image_data["labels"], list):
                        image_data["labels"] = ", ".join(image_data["labels"])
                    image_data["cluster_id"] = cluster_id
                    all_images.append(image_data)
                page_number += 1

        if all_images:
            df = pd.DataFrame(all_images)
            return df
        else:
            return pd.DataFrame()

    def search_by_labels(self, labels: List[str]) -> pd.DataFrame:
        """
        Search dataset by labels and return all images as a DataFrame.

        Args:
            labels (List[str]): List of labels to search for, e.g., ["cat", "dog"]

        Returns:
            pd.DataFrame: DataFrame containing all images from matching clusters
        """
        if not labels:
            return pd.DataFrame()

        # --- Paginate cluster retrieval ---
        cluster_ids = []
        all_cluster_ids = set()
        page_number = 0
        while True:
            params = {"labels": json.dumps(labels), "page_number": page_number}
            response = self.client.session.get(
                f"{self.base_url}/explore/{self.dataset_id}",
                params=params,
                headers=self.client._get_headers(),
            )
            response.raise_for_status()
            data = response.json()
            clusters = data.get("clusters", [])
            if not clusters:
                break
            for cluster in clusters:
                cluster_id = cluster.get("cluster_id")
                if cluster_id and cluster_id not in all_cluster_ids:
                    cluster_ids.append(cluster_id)
                    all_cluster_ids.add(cluster_id)
            page_number += 1

        # --- Paginate image retrieval for each cluster ---
        all_images = []
        seen_image_ids = set()
        for cluster_id in cluster_ids:
            page_number = 0
            while True:
                cluster_params = {"verbose": "true", "labels": json.dumps(labels), "page_number": page_number}
                cluster_response = self.client.session.get(
                    f"{self.base_url}/explore/{self.dataset_id}/similarity_cluster/{cluster_id}",
                    params=cluster_params,
                    headers=self.client._get_headers(),
                )
                cluster_response.raise_for_status()
                cluster_data = cluster_response.json()
                if cluster_data is None:
                    break
                previews = cluster_data.get("previews", [])
                if not previews:
                    break
                for preview in previews:
                    image_id = preview.get("image_id") or preview.get("id")
                    if image_id and image_id in seen_image_ids:
                        continue
                    if image_id:
                        seen_image_ids.add(image_id)
                    image_data = preview.copy()
                    image_labels = preview.get("labels", [])
                    if isinstance(image_labels, list):
                        image_data["labels"] = ", ".join(image_labels)
                    image_data["cluster_id"] = cluster_id
                    all_images.append(image_data)
                page_number += 1

        if all_images:
            df = pd.DataFrame(all_images)
            return df
        else:
            return pd.DataFrame()

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

    def search_by_captions_async(self, caption_text: str, similarity_threshold: float = 0.83) -> dict:
        """
        Search dataset by AI-generated captions using the export_context_async endpoint.
        Returns the export task ID for polling status.

        Args:
            caption_text (str): Text to search in captions
            similarity_threshold (float): Threshold for semantic search (default: 0.83)

        Returns:
            dict: Export task response with task ID
        """
        if not caption_text:
            self.logger.warning("No caption text provided")
            return {}

        url = f"{self.base_url}/dataset/{self.dataset_id}/export_context_async"

        params = {"export_format": "json", "include_images": False, "caption_only_filter": caption_text}

        try:
            self.logger.info(f"Searching captions for: '{caption_text}' with threshold: {similarity_threshold}")
            self.logger.info(f"API URL: {url}")
            self.logger.info(f"API Parameters: {params}")

            response = self.client.session.get(url, headers=self.client._get_headers(), params=params)
            response.raise_for_status()

            result = response.json()

            # Print raw API response
            print(f"\n📄 RAW API RESPONSE for caption search:")
            print(f"Response type: {type(result)}")
            print(f"Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
            print(f"Full response: {json.dumps(result, indent=2)}")

            self.logger.success(f"Caption search export task created successfully")
            return result

        except Exception as e:
            self.logger.error(f"Caption search failed: {str(e)}")
            raise

    def search_by_labels_async(self, labels: List[str]) -> dict:
        """
        Search dataset by labels using the export_context_async endpoint, then poll export_status.
        Returns the export status response (including download_uri if ready).

        Args:
            labels (List[str]): List of labels to search for, e.g., ["cat", "dog"]

        Returns:
            dict: Export status response with progress, status, and download_uri
        """
        if not labels:
            self.logger.warning("No labels provided")
            return {}

        # Step 1: Start export task
        url_context = f"{self.base_url}/dataset/{self.dataset_id}/export_context_async"
        params = {"export_format": "json", "include_images": False, "labels": json.dumps(labels)}

        try:
            self.logger.info(f"Starting label search export task: {labels}")
            response = self.client.session.get(url_context, headers=self.client._get_headers(), params=params)
            response.raise_for_status()
            result = response.json()
            self.logger.success(f"Label search export task created successfully")
        except Exception as e:
            self.logger.error(f"Label search export_context_async failed: {str(e)}")
            raise

        # Step 2: Poll export status
        export_task_id = result.get("id")
        if not export_task_id:
            self.logger.error("No export_task_id returned from export_context_async")
            return {}

        url_status = f"{self.base_url}/dataset/{self.dataset_id}/export_status"
        status_params = {"export_task_id": export_task_id, "dataset_id": self.dataset_id}
        try:
            self.logger.info(f"Polling export status for task: {export_task_id}")
            status_response = self.client.session.get(url_status, headers=self.client._get_headers(), params=status_params)
            status_response.raise_for_status()
            status_result = status_response.json()
            self.logger.success(f"Export status checked successfully")
            return status_result
        except Exception as e:
            self.logger.error(f"Export status check failed: {str(e)}")
            raise

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

    def search_by_labels_async_to_dataframe(self, labels: List[str], poll_interval: int = 10, timeout: int = 300) -> pd.DataFrame:
        """
        Search dataset by labels asynchronously, poll until export is ready, download the results, and return as a DataFrame.

        Args:
            labels (List[str]): List of labels to search for
            poll_interval (int): Seconds to wait between status polls (default: 10)
            timeout (int): Maximum seconds to wait for export to complete (default: 300)

        Returns:
            pd.DataFrame: DataFrame containing the search results, or empty if not ready
        """
        import time

        start_time = time.time()

        # Step 1: Start async search and get initial status
        status_result = self.search_by_labels_async(labels)
        download_uri = status_result.get("download_uri")
        status = status_result.get("status")
        export_task_id = status_result.get("id")

        # Poll if not ready
        while (status != "COMPLETED" or not download_uri) and (time.time() - start_time < timeout):
            self.logger.info(f"Export not ready (status: {status}). Waiting {poll_interval}s before polling again...")
            time.sleep(poll_interval)
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

        if status != "COMPLETED" or not download_uri:
            self.logger.warning(f"Export not completed or no download_uri after waiting. Final status: {status}")
            return pd.DataFrame()

        # Step 2: Use the general processor
        return self.process_export_download_to_dataframe(download_uri)

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
