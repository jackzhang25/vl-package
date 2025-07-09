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

    try:
        # Test 1: API Health Check
        print("\n" + "=" * 60)
        print("TEST 1: API Health Check")
        print("=" * 60)

        health_status = client.healthcheck()
        client.logger.api_health_check(health_status)

        # Test 2: Get Sample Datasets
        print("\n" + "=" * 60)
        print("TEST 2: Get Sample Datasets")
        print("=" * 60)

        try:
            sample_datasets = client.get_sample_datasets()
            client.logger.success(f"Retrieved {len(sample_datasets)} sample datasets")

            if sample_datasets:
                client.logger.info(f"Sample dataset names:")
                for i, dataset in enumerate(sample_datasets[:5], 1):  # Show first 5
                    name = dataset.get("display_name", "Unnamed Dataset")
                    client.logger.info(f"  {i}. {name}")

        except Exception as e:
            client.logger.error(f"Failed to get sample datasets: {str(e)}")

        # Test 3: Get All Datasets
        print("\n" + "=" * 60)
        print("TEST 3: Get All Datasets")
        print("=" * 60)

        try:
            client.logger.info("Fetching all datasets...")
            all_datasets = client.get_all_datasets()
            client.logger.success(f"Retrieved {len(all_datasets)} total datasets")

            if len(all_datasets) > 0:
                client.logger.info(f"Dataset status breakdown:")
                status_counts = all_datasets["status"].value_counts()
                for status, count in status_counts.items():
                    client.logger.info(f"  {status}: {count} datasets")

        except Exception as e:
            client.logger.error(f"Failed to get all datasets: {str(e)}")

        # Test 4: Dataset Operations Simulation
        print("\n" + "=" * 60)
        print("TEST 4: Dataset Operations Simulation")
        print("=" * 60)

        # Simulate dataset creation process
        client.logger.info("Simulating dataset creation process...")
        client.logger.dataset_created("test-dataset-123", "My Test Dataset")

        # Simulate upload process
        client.logger.dataset_uploading("My Test Dataset")
        client.logger.dataset_uploaded("My Test Dataset")

        # Simulate processing
        client.logger.dataset_processing("My Test Dataset")
        client.logger.dataset_ready("My Test Dataset")

        # Test 5: Search Operations Simulation
        print("\n" + "=" * 60)
        print("TEST 5: Search Operations Simulation")
        print("=" * 60)

        # Simulate label search
        client.logger.search_started("labels", "cat")
        client.logger.search_completed(42, "labels", "cat")

        client.logger.search_started("labels", "dog")
        client.logger.search_completed(28, "labels", "dog")

        # Simulate caption search
        client.logger.search_started("captions", "people")
        client.logger.search_completed(0, "captions", "people")

        client.logger.search_started("captions", "outdoor")
        client.logger.search_completed(156, "captions", "outdoor")

        # Test 6: Export Operations Simulation
        print("\n" + "=" * 60)
        print("TEST 6: Export Operations Simulation")
        print("=" * 60)

        client.logger.export_started("test-dataset-123")
        client.logger.export_completed("test-dataset-123", 150)

        client.logger.export_started("test-dataset-456")
        client.logger.export_failed("test-dataset-456", "Dataset not found")

        # Test 7: Error Handling Examples
        print("\n" + "=" * 60)
        print("TEST 7: Error Handling Examples")
        print("=" * 60)

        client.logger.warning("Dataset is not ready for export")
        client.logger.dataset_not_ready("test-dataset-789", "processing")

        client.logger.error("Failed to connect to API")
        client.logger.request_error("Connection timeout")

        client.logger.warning("Found duplicate images in search results")
        client.logger.info("Proceeding with deduplication...")

        # Test 8: Verbose Logging Demo
        print("\n" + "=" * 60)
        print("TEST 8: Verbose Logging Demo")
        print("=" * 60)

        from visual_layer_sdk.logger import set_verbose

        client.logger.info("Enabling verbose logging for detailed output...")
        set_verbose(True)

        try:
            # This will now show detailed request information
            client.logger.info("Making a request with verbose logging enabled...")
            health_status = client.healthcheck()
            client.logger.api_health_check(health_status)

        except Exception as e:
            client.logger.error(f"Error in verbose logging demo: {str(e)}")

        # Reset to normal logging
        set_verbose(False)
        client.logger.info("Verbose logging disabled - returning to normal output")

        # Test 9: Real Dataset Operations (if test dataset exists)
        print("\n" + "=" * 60)
        print("TEST 9: Real Dataset Operations")
        print("=" * 60)

        test_dataset_id = "5db7f426-4fdf-11ef-8d8b-5e82a4538d0f"

        try:
            client.logger.info(f"Testing with dataset: {test_dataset_id}")
            test_dataset = Dataset(client, test_dataset_id)

            # Get dataset details
            details = test_dataset.get_details()
            dataset_name = details.get("display_name", "Unknown Dataset")
            status = details.get("status", "unknown")

            client.logger.info(f"Dataset: {dataset_name} (Status: {status})")

            # Test label search
            client.logger.search_started("labels", "table")
            table_results = test_dataset.search_by_labels(["table"])
            client.logger.search_completed(len(table_results), "labels", "table")

            if len(table_results) > 0:
                client.logger.info(f"Found {len(table_results)} images with 'table' label")
                client.logger.info(f"DataFrame shape: {table_results.shape}")

                # Check for duplicates
                if "image_id" in table_results.columns:
                    unique_images = table_results["image_id"].nunique()
                    if len(table_results) != unique_images:
                        client.logger.warning(f"Found {len(table_results) - unique_images} duplicate images")
                    else:
                        client.logger.success("No duplicate images found")

            # Test caption search
            client.logger.search_started("captions", "people")
            people_results = test_dataset.search_by_captions("people")
            client.logger.search_completed(len(people_results), "captions", "people")

            if len(people_results) > 0:
                client.logger.info(f"Found {len(people_results)} images with 'people' in caption")
                client.logger.info(f"DataFrame shape: {people_results.shape}")

        except Exception as e:
            client.logger.error(f"Error testing real dataset operations: {str(e)}")

    except requests.exceptions.RequestException as e:
        client.logger.error(f"Request Error: {str(e)}")
    except Exception as e:
        client.logger.error(f"Unexpected error: {str(e)}")

    print("\n" + "=" * 60)
    print("✅ Logger Testing Complete!")
    print("=" * 60)

    print("\n📝 Summary of Logger Features Demonstrated:")
    print("• ✅ Success messages with checkmark emoji")
    print("• 📤 Upload progress indicators")
    print("• 🔍 Search operation status")
    print("• ⚠️  Warning messages for potential issues")
    print("• ❌ Error messages for failures")
    print("• 🔄 Processing status updates")
    print("• 📊 Data statistics and summaries")
    print("• 🐛 Verbose debugging information")
    print("• 🎯 Natural language descriptions")

    print("\n🚀 The logging system provides:")
    print("• Better user experience with clear status updates")
    print("• Professional appearance with consistent formatting")
    print("• Easy debugging with configurable verbosity")
    print("• Comprehensive error reporting")
    print("• Natural language output instead of technical jargon")


if __name__ == "__main__":
    main()
