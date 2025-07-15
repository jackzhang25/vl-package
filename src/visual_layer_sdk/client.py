import os
from datetime import datetime, timedelta, timezone

import jwt
import pandas as pd
import requests
from dotenv import load_dotenv

from .dataset import Dataset
from .logger import get_logger


class VisualLayerClient:
    def __init__(self, api_key: str, api_secret: str):
        self.base_url = "https://app.visual-layer.com/api/v1"
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
        self.logger = get_logger()

    def _generate_jwt(self) -> str:
        jwt_algorithm = "HS256"
        jwt_header = {
            "alg": jwt_algorithm,
            "typ": "JWT",
            "kid": self.api_key,
        }

        now = datetime.now(tz=timezone.utc)
        expiration = now + timedelta(minutes=10)

        payload = {
            "sub": self.api_key,
            "iat": int(now.timestamp()),
            "exp": int(expiration.timestamp()),
            "iss": "sdk",
        }

        return jwt.encode(
            payload=payload,
            key=self.api_secret,
            algorithm=jwt_algorithm,
            headers=jwt_header,
        )

    def _get_headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._generate_jwt()}",
            "accept": "application/json",
            "Content-Type": "application/json",
        }

    def _get_headers_no_jwt(self) -> dict:
        return {"accept": "application/json", "Content-Type": "application/json"}

    def get_sample_datasets(self) -> list:
        """Get sample datasets"""
        url = f"{self.base_url}/datasets/sample_data"
        headers = self._get_headers()

        self.logger.request_details(url, "GET")
        self.logger.debug(f"Headers: {headers}")
        self.logger.debug(f"JWT Token: {self._generate_jwt()}")

        try:
            self.logger.info("Fetching sample datasets...")
            response = self.session.get(url, headers=headers, timeout=10)
            self.logger.request_success(response.status_code)
            self.logger.debug(f"Response Headers: {dict(response.headers)}")
            self.logger.debug(f"Response Body: {response.text[:500]}...")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            self.logger.error("Request timed out after 10 seconds")
            raise
        except requests.exceptions.RequestException as e:
            self.logger.request_error(str(e))
            if hasattr(e, "response"):
                self.logger.debug(f"Error response: {e.response.text}")
            raise

    def healthcheck(self) -> dict:
        """Check the health of the API"""
        response = self.session.get(f"{self.base_url}/healthcheck", headers=self._get_headers())
        response.raise_for_status()
        return response.json()

    # TODO: consider adding a limit to the number of datasets returned
    def get_all_datasets(self) -> pd.DataFrame:
        """Get all datasets as a DataFrame"""
        response = self.session.get(f"{self.base_url}/datasets", headers=self._get_headers())
        response.raise_for_status()
        datasets = response.json()

        # Select only the specific fields for each dataset
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
            "n_images",
        ]

        # Filter each dataset to only include the selected fields
        filtered_datasets = []
        for dataset in datasets:
            filtered_dataset = {field: dataset.get(field) for field in selected_fields}
            filtered_datasets.append(filtered_dataset)

        # Convert to DataFrame
        df = pd.DataFrame(filtered_datasets)
        return df

    def get_dataset(self, dataset_id: str) -> pd.DataFrame:
        """Get dataset details as a DataFrame for the given ID"""
        return self.get_dataset_details_as_dataframe(dataset_id)

    # TODO: move to dataset.py
    def get_dataset_details_as_dataframe(self, dataset_id: str) -> pd.DataFrame:
        """Get dataset details as a DataFrame for the given ID"""
        response = self.session.get(f"{self.base_url}/dataset/{dataset_id}", headers=self._get_headers())
        response.raise_for_status()
        dataset_details = response.json()

        # Select only the specific fields requested
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
            "n_images",
        ]

        # Filter the dataset details to only include the selected fields
        filtered_details = {field: dataset_details.get(field) for field in selected_fields}

        # Convert to DataFrame with a single row
        df = pd.DataFrame([filtered_details])
        return df

    def get_dataset_object(self, dataset_id: str) -> Dataset:
        """Get a dataset object for the given ID (for operations like export, delete, etc.)"""
        return Dataset(self, dataset_id)

    # TODO: validate inputs
    def create_dataset_from_s3_bucket(self, s3_bucket_path: str, dataset_name: str, pipeline_type: str = None) -> Dataset:
        """
        Create a dataset from an S3 bucket.

        Args:
            s3_bucket_path (str): Path to the S3 bucket containing files for processing
            dataset_name (str): The desired name of the dataset
            pipeline_type (str, optional): Type of pipeline to use for processing

        Returns:
            Dataset: Dataset object for the created dataset

        Raises:
            requests.exceptions.RequestException: If the request fails
            ValueError: If the path or name is invalid
        """
        if not s3_bucket_path or not dataset_name:
            raise ValueError("Both s3_bucket_path and dataset_name are required")

        url = f"{self.base_url}/dataset"

        # Prepare form data with all required fields
        form_data = {
            "dataset_name": dataset_name,
            "vl_dataset_id": "",
            "bucket_path": s3_bucket_path,
            "uploaded_filename": "",
            "config_url": "",
            "pipeline_type": pipeline_type if pipeline_type else "",
        }

        try:
            headers = self._get_headers()
            headers["Content-Type"] = "application/x-www-form-urlencoded"

            self.logger.request_details(url, "POST")
            self.logger.debug(f"Form Data: {form_data}")

            self.logger.info(f"Creating dataset '{dataset_name}' from S3 bucket...")
            response = self.session.post(
                url,
                data=form_data,  # Use data parameter for form data
                headers=headers,
                timeout=30,  # Increased timeout for processing
            )

            self.logger.request_success(response.status_code)
            self.logger.debug(f"Response Body: {response.text}")

            response.raise_for_status()
            result = response.json()

            if result.get("status") == "error":
                raise requests.exceptions.RequestException(result.get("message", "Unknown error"))

            # Extract dataset ID and return Dataset object
            dataset_id = result.get("id")
            if not dataset_id:
                raise requests.exceptions.RequestException("No dataset_id returned from creation")

            self.logger.dataset_created(dataset_id, dataset_name)
            return Dataset(self, dataset_id)

        except requests.exceptions.Timeout:
            raise requests.exceptions.RequestException("Request timed out - dataset processing may take longer than expected")
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise requests.exceptions.RequestException(error_data.get("message", str(e)))
                except ValueError:
                    pass
            raise

    def create_dataset_from_local_folder(
        self,
        file_path: str,
        filename: str,
        dataset_name: str,
        pipeline_type: str = None,
    ) -> Dataset:
        """
        Create a dataset from a local zip file.

        Args:
            file_path (str): Full path to the zip file (e.g., "/path/to/images.zip")
            filename (str): Name of the zip file (e.g., "images.zip")
            dataset_name (str): The desired name of the dataset
            pipeline_type (str, optional): Type of pipeline to use for processing

        Returns:
            Dataset: Dataset object for the created dataset

        Raises:
            requests.exceptions.RequestException: If the request fails
            ValueError: If the file path, filename, or name is invalid
        """
        if not file_path or not filename or not dataset_name:
            raise ValueError("file_path, filename, and dataset_name are all required")

        # Check if file exists
        if not os.path.exists(file_path):
            raise ValueError(f"File not found: {file_path}")

        # Step 1: Create the dataset
        url = f"{self.base_url}/dataset"

        # Prepare form data for dataset creation
        form_data = {
            "dataset_name": dataset_name,
            "vl_dataset_id": "",
            "bucket_path": "",
            "uploaded_filename": filename,
            "config_url": "",
            "pipeline_type": pipeline_type if pipeline_type else "",
        }

        try:
            headers = self._get_headers()
            headers["Content-Type"] = "application/x-www-form-urlencoded"

            self.logger.info(f"Creating dataset '{dataset_name}'...")
            self.logger.request_details(url, "POST")
            self.logger.debug(f"Form Data: {form_data}")

            response = self.session.post(url, data=form_data, headers=headers)

            self.logger.request_success(response.status_code)
            response.raise_for_status()
            result = response.json()

            if result.get("status") == "error":
                raise requests.exceptions.RequestException(result.get("message", "Unknown error"))

            dataset_id = result.get("id")
            if not dataset_id:
                raise requests.exceptions.RequestException("No dataset_id returned from creation")

            self.logger.dataset_created(dataset_id, dataset_name)

            # Step 2: Upload the zip file to the dataset
            upload_url = f"{self.base_url}/dataset/{dataset_id}/upload"

            self.logger.dataset_uploading(dataset_name)
            self.logger.request_details(upload_url, "POST")
            self.logger.debug(f"File path: {file_path}")
            self.logger.debug(f"Filename: {filename}")

            # Prepare multipart form data for file upload
            with open(file_path, "rb") as file:
                files = {"file": (filename, file, "application/zip")}
                data = {"operations": "READ"}

                upload_headers = self._get_headers()
                # Remove Content-Type header to let requests set it for multipart
                upload_headers.pop("Content-Type", None)

                upload_response = self.session.post(
                    upload_url,
                    files=files,
                    data=data,
                    headers=upload_headers,
                )

                self.logger.request_success(upload_response.status_code)
                upload_response.raise_for_status()
                upload_result = upload_response.json()

                self.logger.dataset_uploaded(dataset_name)

                # Return Dataset object
                return Dataset(self, dataset_id)
                # TODO: return dataset object instead of dict
        except requests.exceptions.Timeout:
            raise requests.exceptions.RequestException("Request timed out - dataset processing may take longer than expected")
        except requests.exceptions.RequestException as e:
            if hasattr(e, "response") and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise requests.exceptions.RequestException(error_data.get("message", str(e)))
                except ValueError:
                    pass
            raise
        except FileNotFoundError:
            raise ValueError(f"Zip file not found: {file_path}")
        except Exception as e:
            raise requests.exceptions.RequestException(f"Unexpected error: {str(e)}")


def main():
    load_dotenv()

    # Get API credentials from environment
    API_KEY = os.getenv("VISUAL_LAYER_API_KEY")
    API_SECRET = os.getenv("VISUAL_LAYER_API_SECRET")

    if not API_KEY or not API_SECRET:
        print("❌ Error: API credentials not found in environment variables")
        print("Please make sure VISUAL_LAYER_API_KEY and VISUAL_LAYER_API_SECRET are set in your .env file")
        return

    print("🚀 Initializing Visual Layer client...")
    client = VisualLayerClient(API_KEY, API_SECRET)

    # Only run the async label search test
    print("\n" + "=" * 60)
    print("TEST: Async Label Search and Download")
    print("=" * 60)

    try:
        test_dataset_id = "bc41491e-78ae-11ef-ba4b-8a774758b536"
        test_labels = ["bean_rust", "angular_leaf_spot"]
        client.logger.info(f"Testing async label search for dataset: {test_dataset_id} with labels: {test_labels}")
        test_dataset = Dataset(client, test_dataset_id)
        df = test_dataset.search_by_labels_async_to_dataframe(test_labels)
        client.logger.success(f"Async label search DataFrame shape: {df.shape}")
        print(df.head())

        # Save DataFrame to CSV
        csv_filename = "async_label_search_results.csv"
        df.to_csv(csv_filename, index=False)
        client.logger.success(f"DataFrame saved to {csv_filename}")
        print(f"📄 Results saved to: {csv_filename}")

    except Exception as e:
        client.logger.error(f"Error in async label search and download: {str(e)}")


if __name__ == "__main__":
    main()
