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
