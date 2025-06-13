from typing import Optional, List, Union
from pathlib import Path
from .client import VisualLayerClient
from .exceptions import VisualLayerError

class Dataset:
    def __init__(self, client: VisualLayerClient):
        """
        Initialize the Dataset class.

        Args:
            client (VisualLayerClient): The Visual Layer API client instance.
        """
        self.client = client

    def create_dataset(
        self,
        dataset_name: str,
        vl_dataset_id: Optional[str] = None,
        bucket_path: Optional[str] = None,
        uploaded_filename: Optional[str] = None,
        config_url: Optional[str] = None,
        pipeline_type: Optional[str] = None
    ) -> str:
        """
        Create a new dataset.

        Args:
            dataset_name (str): Name of the dataset.
            vl_dataset_id (Optional[str]): ID of a sample dataset to grant access to.
            bucket_path (Optional[str]): Path to the bucket containing the dataset.
            uploaded_filename (Optional[str]): Name of the uploaded file.
            config_url (Optional[str]): URL to the configuration file.
            pipeline_type (Optional[str]): Type of processing pipeline to use.

        Returns:
            str: The ID of the newly created dataset.

        Raises:
            VisualLayerError: If dataset creation fails.
        """
        try:
            # Create form data with required and optional parameters
            form_data = {
                "dataset_name": dataset_name
            }
            
            # Add optional parameters if provided
            if vl_dataset_id is not None:
                form_data["vl_dataset_id"] = vl_dataset_id
            if bucket_path is not None:
                form_data["bucket_path"] = bucket_path
            if uploaded_filename is not None:
                form_data["uploaded_filename"] = uploaded_filename
            if config_url is not None:
                form_data["config_url"] = config_url
            if pipeline_type is not None:
                form_data["pipeline_type"] = pipeline_type

            # Send POST request with form data
            response = self.client.post("/dataset", data=form_data)
            return response["id"]
        except Exception as e:
            raise VisualLayerError(f"Failed to create dataset: {str(e)}")

    def upload_images(self, dataset_id: str, image_paths: Union[str, Path, List[Union[str, Path]]]) -> str:
        """
        Upload images to a dataset.

        Args:
            dataset_id (str): The ID of the dataset to upload to.
            image_paths (Union[str, Path, List[Union[str, Path]]]): Path or list of paths to images to upload.

        Returns:
            str: The transaction ID for the upload.

        Raises:
            VisualLayerError: If the upload fails.
        """
        try:
            # Convert single path to list
            if isinstance(image_paths, (str, Path)):
                image_paths = [image_paths]

            # Start upload transaction
            response = self.client.post(f"/ingestion/{dataset_id}/data_files")
            transaction_id = response["transaction_id"]

            # Upload each image
            for image_path in image_paths:
                path = Path(image_path)
                if not path.exists():
                    raise VisualLayerError(f"Image file not found: {path}")

                with open(path, 'rb') as f:
                    files = {'file': (path.name, f)}
                    self.client.post(
                        f"/ingestion/{dataset_id}/data_files/{transaction_id}",
                        files=files
                    )

            # Process the uploaded files
            self.client.post(f"/ingestion/{dataset_id}/process_files/{transaction_id}")

            return transaction_id
        except Exception as e:
            raise VisualLayerError(f"Failed to upload images: {str(e)}")

    def get_upload_status(self, dataset_id: str, transaction_id: str) -> dict:
        """
        Get the status of an upload transaction.

        Args:
            dataset_id (str): The ID of the dataset.
            transaction_id (str): The transaction ID from the upload.

        Returns:
            dict: The status of the upload transaction.

        Raises:
            VisualLayerError: If getting the status fails.
        """
        try:
            response = self.client.get(f"/ingestion/{dataset_id}/data_files/{transaction_id}")
            return response
        except Exception as e:
            raise VisualLayerError(f"Failed to get upload status: {str(e)}") 