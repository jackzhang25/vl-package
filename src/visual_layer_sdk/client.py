import os
from datetime import datetime, timedelta, timezone

import jwt
import pandas as pd
import requests
from dotenv import load_dotenv

from .dataset import Dataset


class VisualLayerClient:
    def __init__(self, api_key: str, api_secret: str):
        self.base_url = "https://app.visual-layer.com/api/v1"
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()

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

        print("\n=== Request Details ===")
        print(f"Full URL: {url}")
        print(f"Headers: {headers}")
        print(f"JWT Token: {self._generate_jwt()}")

        try:
            print("\nMaking request...")
            response = self.session.get(url, headers=headers, timeout=10)
            print(f"Response Status: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Body: {response.text[:500]}...")

            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print("\nRequest timed out after 10 seconds")
            raise
        except requests.exceptions.RequestException as e:
            print(f"\nRequest failed: {str(e)}")
            if hasattr(e, "response"):
                print(f"Error response: {e.response.text}")
            raise

    def healthcheck(self) -> dict:
        """Check the health of the API"""
        response = self.session.get(f"{self.base_url}/healthcheck", headers=self._get_headers())
        response.raise_for_status()
        return response.json()

    # TODO: consider adding a limit to the number of datasets returned
    # TODO: return a dataframe instead of json
    def get_all_datasets(self) -> list:
        """Get all datasets"""
        response = self.session.get(f"{self.base_url}/datasets", headers=self._get_headers())
        response.raise_for_status()
        return response.json()

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
    def create_dataset_from_s3_bucket(self, s3_bucket_path: str, dataset_name: str, pipeline_type: str = None) -> dict:
        """
        Create a dataset from an S3 bucket.

        Args:
            s3_bucket_path (str): Path to the S3 bucket containing files for processing
            dataset_name (str): The desired name of the dataset
            pipeline_type (str, optional): Type of pipeline to use for processing

        Returns:
            dict: Response containing dataset information

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

            print("\n=== Request Details ===")
            print(f"URL: {url}")
            print(f"Headers: {headers}")
            print(f"Form Data: {form_data}")

            response = self.session.post(
                url,
                data=form_data,  # Use data parameter for form data
                headers=headers,
                timeout=30,  # Increased timeout for processing
            )

            print(f"\nResponse Status: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Body: {response.text}")

            response.raise_for_status()
            result = response.json()

            if result.get("status") == "error":
                raise requests.exceptions.RequestException(result.get("message", "Unknown error"))

            return result

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
    ) -> dict:
        """
        Create a dataset from a local zip file.

        Args:
            file_path (str): Full path to the zip file (e.g., "/path/to/images.zip")
            filename (str): Name of the zip file (e.g., "images.zip")
            dataset_name (str): The desired name of the dataset
            pipeline_type (str, optional): Type of pipeline to use for processing

        Returns:
            dict: Response containing dataset information

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

            print("\n=== Step 1: Creating Dataset ===")
            print(f"URL: {url}")
            print(f"Form Data: {form_data}")

            response = self.session.post(url, data=form_data, headers=headers)

            print(f"Response Status: {response.status_code}")
            response.raise_for_status()
            result = response.json()
            print(result)
            if result.get("status") == "error":
                raise requests.exceptions.RequestException(result.get("message", "Unknown error"))

            dataset_id = result.get("id")
            if not dataset_id:
                raise requests.exceptions.RequestException("No dataset_id returned from creation")

            print(f"Dataset created with ID: {dataset_id}")

            # Step 2: Upload the zip file to the dataset
            upload_url = f"{self.base_url}/dataset/{dataset_id}/upload"

            print("\n=== Step 2: Uploading Zip File ===")
            print(f"Upload URL: {upload_url}")
            print(f"File path: {file_path}")
            print(f"Filename: {filename}")

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

                print(f"Upload Response Status: {upload_response.status_code}")
                upload_response.raise_for_status()
                upload_result = upload_response.json()

                print(f"Upload successful: {upload_result}")

                # Return the combined result
                result["upload_result"] = upload_result
                return result
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
        print("Error: API credentials not found in environment variables")
        print("Please make sure VISUAL_LAYER_API_KEY and VISUAL_LAYER_API_SECRET are set in your .env file")
        return

    print("Initializing Visual Layer client...")
    client = VisualLayerClient(API_KEY, API_SECRET)

    try:
        # Check API health
        print("\nChecking API health...")
        health_status = client.healthcheck()
        print(f"API Health Status: {health_status}")

        # Test creating dataset from local zip file
        print("\n=== Testing Local Zip File Dataset Creation ===")
        file_path = "/Users/Jack/Downloads/archive/images.zip"
        filename = "images.zip"
        dataset_name = "test_local_zip_dataset"

        print(f"File path: {file_path}")
        print(f"Filename: {filename}")
        print(f"Dataset name: {dataset_name}")

        try:
            result = client.create_dataset_from_local_folder(file_path, filename, dataset_name)
            print(f"\nDataset creation result: {result}")

            # If successful, get the dataset ID and test other operations
            if result.get("dataset_id"):
                dataset_id = result["dataset_id"]
                print(f"\nCreated dataset with ID: {dataset_id}")

                # Get dataset details as DataFrame
                dataset_df = client.get_dataset(dataset_id)
                print("\nDataset details:")
                print(dataset_df.to_string(index=False))

                # Save dataset details to CSV
                dataset_csv_filename = f"dataset_{dataset_id}_details.csv"
                dataset_df.to_csv(dataset_csv_filename, index=False)
                print(f"\nSaved dataset details to: {dataset_csv_filename}")

        except Exception as e:
            print(f"Error creating dataset from local zip: {str(e)}")

    except requests.exceptions.RequestException as e:
        print(f"\nError: {str(e)}")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")


if __name__ == "__main__":
    main()
