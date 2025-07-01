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

            # Print the result instead of returning it
            print(f"\nDataset creation result: {result}")

            # Extract dataset ID and return Dataset object
            dataset_id = result.get("id")
            if not dataset_id:
                raise requests.exceptions.RequestException("No dataset_id returned from creation")

            print(f"Created dataset with ID: {dataset_id}")
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

                # Print the combined result instead of returning it
                result["upload_result"] = upload_result
                print(f"\nDataset creation result: {result}")

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
        print("Error: API credentials not found in environment variables")
        print("Please make sure VISUAL_LAYER_API_KEY and VISUAL_LAYER_API_SECRET are set in your .env file")
        return

    print("Initializing Visual Layer client...")
    client = VisualLayerClient(API_KEY, API_SECRET)

    try:
        # Check API health first
        print("\n=== Checking API Health ===")
        health_status = client.healthcheck()
        print(f"✅ API Health Status: {health_status}")

        # Test label search for "french_fries"
        print("\n=== Testing Label Search for 'french_fries' ===")
        try:
            # Use the specified dataset ID
            test_dataset_id = "148a39c2-6154-11ef-a4e6-aa6d4667de12"
            test_dataset = Dataset(client, test_dataset_id)

            print(f"Testing dataset: {test_dataset_id}")
            print("Searching for images with 'french_fries' label...")

            # Debug: Direct API call to see what we get
            print("\n--- Debug: Direct API Call ---")
            import json

            params = {"labels": json.dumps(["french_fries"])}
            response = client.session.get(
                f"{client.base_url}/explore/{test_dataset_id}",
                params=params,
                headers=client._get_headers(),
            )
            response.raise_for_status()
            data = response.json()

            print(f"First API call - clusters found: {len(data.get('clusters', []))}")
            if data.get("clusters"):
                total_previews_first = sum(len(cluster.get("previews", [])) for cluster in data["clusters"])
                print(f"First API call - total previews: {total_previews_first}")

                # Count images with 'french_fries' label in first call
                french_fries_count_first = 0
                for cluster in data["clusters"]:
                    for preview in cluster.get("previews", []):
                        labels = preview.get("labels", [])
                        if "french_fries" in labels:
                            french_fries_count_first += 1
                print(f"First API call - images with 'french_fries' label: {french_fries_count_first}")

                # Test second API call for first few clusters
                print(f"\n--- Debug: Second API Calls ---")
                for i, cluster in enumerate(data["clusters"][:3]):  # Test first 3 clusters
                    cluster_id = cluster.get("cluster_id")
                    if cluster_id:
                        print(f"Cluster {i+1} ({cluster_id}):")
                        cluster_params = {"verbose": "true", "labels": json.dumps(["french_fries"])}
                        cluster_response = client.session.get(
                            f"{client.base_url}/explore/{test_dataset_id}/similarity_cluster/{cluster_id}",
                            params=cluster_params,
                            headers=client._get_headers(),
                        )
                        cluster_response.raise_for_status()
                        cluster_data = cluster_response.json()

                        print(f"  Second API call - previews in cluster: {len(cluster_data.get('previews', []))}")

                        # Count images with 'french_fries' label in second call
                        french_fries_count_second = 0
                        for preview in cluster_data.get("previews", []):
                            labels = preview.get("labels", [])
                            if "french_fries" in labels:
                                french_fries_count_second += 1
                        print(f"  Second API call - images with 'french_fries' label: {french_fries_count_second}")

            print(f"\n--- Debug: Our Implementation ---")
            french_fries_results = test_dataset.search_by_labels(["french_fries"])
            print(f"✅ Found {len(french_fries_results)} images with 'french_fries' label")

            if len(french_fries_results) > 0:
                print(f"DataFrame shape: {french_fries_results.shape}")
                print(f"Columns: {list(french_fries_results.columns)}")
                print(f"Unique cluster IDs: {french_fries_results['cluster_id'].nunique()}")

                # Check for duplicate images
                if "image_id" in french_fries_results.columns:
                    unique_images = french_fries_results["image_id"].nunique()
                    print(f"Unique image IDs: {unique_images}")
                    if len(french_fries_results) != unique_images:
                        print(f"⚠️  WARNING: Found {len(french_fries_results) - unique_images} duplicate images!")

                # Show the actual DataFrame
                print(f"\nDataFrame Preview (first 10 rows):")
                print(french_fries_results.head(10).to_string(index=False))

                # Show sample of labels found
                if "labels" in french_fries_results.columns:
                    print(f"\nSample labels found:")
                    sample_labels = french_fries_results["labels"].head(10).tolist()
                    for i, label in enumerate(sample_labels, 1):
                        print(f"  {i}. {label}")

                # Save DataFrame to CSV for detailed inspection
                csv_filename = "french_fries_label_results.csv"
                french_fries_results.to_csv(csv_filename, index=False)
                print(f"\n✅ Saved detailed results to: {csv_filename}")

            else:
                print("❌ No images found with 'french_fries' label")

        except Exception as e:
            print(f"❌ Error in french_fries label search: {str(e)}")
            import traceback

            print(f"Full error: {traceback.format_exc()}")

    except requests.exceptions.RequestException as e:
        print(f"\n❌ Request Error: {str(e)}")
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")

    print("\n=== Testing Complete ===")


if __name__ == "__main__":
    main()
