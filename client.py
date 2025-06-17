from uuid import UUID
import requests
from datetime import datetime, timezone, timedelta
import jwt
import os
from dotenv import load_dotenv

class Dataset:
    def __init__(self, client, dataset_id: str):
        self.client = client
        self.dataset_id = dataset_id
        self.base_url = client.base_url

    def get_stats(self) -> dict:
        """Get statistics for this dataset"""
        response = self.client.session.get(
            f"{self.base_url}/dataset/{self.dataset_id}/stats",
            headers=self.client._get_headers()
        )
        response.raise_for_status()
        return response.json()

    def get_details(self) -> dict:
        """Get details for this dataset"""
        response = self.client.session.get(
            f"{self.base_url}/dataset/{self.dataset_id}",
            headers=self.client._get_headers_no_jwt()
        )
        response.raise_for_status()
        return response.json()

    def explore(self) -> dict:
        """Explore this dataset"""
        response = self.client.session.get(
            f"{self.base_url}/explore/{self.dataset_id}",
            headers=self.client._get_headers_no_jwt()
        )
        response.raise_for_status()
        return response.json()

    def delete(self) -> dict:
        """Delete this dataset"""
        response = self.client.session.delete(
            f"{self.base_url}/dataset/{self.dataset_id}",
            headers=self.client._get_headers_no_jwt()
        )
        response.raise_for_status()
        return response.json()

class VisualLayerClient:
    def __init__(self, api_key: str, api_secret: str):
        # Use staging environment
        self.base_url = "https://app.visual-layer.com/api/v1"
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()
    
    def _generate_jwt(self) -> str:
        jwt_algorithm = "HS256"
        jwt_header = {
            'alg': jwt_algorithm,
            'typ': 'JWT',
            'kid': self.api_key,
        }
        
        now = datetime.now(tz=timezone.utc)
        expiration = now + timedelta(minutes=10)
        
        payload = {
            'sub': self.api_key,
            'iat': int(now.timestamp()),
            'exp': int(expiration.timestamp()),
            'iss': 'sdk'
        }
        
        return jwt.encode(
            payload=payload, 
            key=self.api_secret, 
            algorithm=jwt_algorithm, 
            headers=jwt_header
        )
    
    def _get_headers(self) -> dict:
        return {
            'Authorization': f'Bearer {self._generate_jwt()}',
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
    def _get_headers_no_jwt(self) -> dict:
        return {
            'accept': 'application/json',
            'Content-Type': 'application/json'
        }
    
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
            if hasattr(e, 'response'):
                print(f"Error response: {e.response.text}")
            raise
    
    def healthcheck(self) -> dict:
        """Check the health of the API"""
        response = self.session.get(
            f"{self.base_url}/healthcheck",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def get_all_datasets(self) -> list:
        """Get all datasets"""
        response = self.session.get(
            f"{self.base_url}/datasets",
            headers=self._get_headers()
        )
        response.raise_for_status() 
        return response.json()

    def get_dataset(self, dataset_id: str) -> Dataset:
        """Get a dataset object for the given ID"""
        return Dataset(self, dataset_id)

    def create_dataset_from_local_folder(self, folder_path: str, dataset_name: str, pipeline_type: str = None) -> dict:
        """
        Create a dataset from a local folder.
        
        Args:
            folder_path (str): Full system path to the folder containing files for processing
            dataset_name (str): The desired name of the dataset
            pipeline_type (str, optional): Type of pipeline to use for processing
            
        Returns:
            dict: Response containing dataset information
            
        Raises:
            requests.exceptions.RequestException: If the request fails
            ValueError: If the path or name is invalid
        """
        if not folder_path or not dataset_name:
            raise ValueError("Both folder_path and dataset_name are required")
            
        if not os.path.exists(folder_path):
            raise ValueError(f"Folder path does not exist: {folder_path}")
            
        if not os.path.isdir(folder_path):
            raise ValueError(f"Path is not a directory: {folder_path}")
            
        url = f"{self.base_url}/dataset"
        
        # Prepare form data with all required fields
        form_data = {
            'dataset_name': dataset_name,
            'vl_dataset_id': '',
            'bucket_path': '',
            'uploaded_filename': folder_path,
            'config_url': '',
            'pipeline_type': pipeline_type if pipeline_type else ''
        }
        
        try:
            headers = self._get_headers()
            headers['Content-Type'] = 'application/x-www-form-urlencoded'
            
            print("\n=== Request Details ===")
            print(f"URL: {url}")
            print(f"Headers: {headers}")
            print(f"Form Data: {form_data}")
            
            response = self.session.post(
                url,
                data=form_data,  # Use data parameter for form data
                headers=headers,
                timeout=30  # Increased timeout for processing
            )
            
            print(f"\nResponse Status: {response.status_code}")
            print(f"Response Headers: {dict(response.headers)}")
            print(f"Response Body: {response.text}")
            
            response.raise_for_status()
            result = response.json()
            
            if result.get('status') == 'error':
                raise requests.exceptions.RequestException(result.get('message', 'Unknown error'))
                
            return result
            
        except requests.exceptions.Timeout:
            raise requests.exceptions.RequestException("Request timed out - dataset processing may take longer than expected")
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    raise requests.exceptions.RequestException(error_data.get('message', str(e)))
                except ValueError:
                    pass
            raise

# Usage example:
def main():
    load_dotenv()

    # Get API credentials from environment
    API_KEY = os.getenv('VISUAL_LAYER_API_KEY')
    API_SECRET = os.getenv('VISUAL_LAYER_API_SECRET')
    
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

        # Example: Create dataset from local folder
        print("\nCreating dataset from local folder...")
        try:
            # Using the specified path
            local_folder = "/Users/Jack/Downloads/archive/images"
            dataset_name = "archive_images_dataset"
            
            print(f"Attempting to create dataset from: {local_folder}")
            print(f"Dataset name: {dataset_name}")
            
            result = client.create_dataset_from_local_folder(local_folder, dataset_name)
            print(f"\nDataset creation started:")
            print(f"- Status: {result.get('status')}")
            print(f"- Dataset ID: {result.get('dataset_id')}")
            print(f"- Name: {result.get('name')}")
            
            # Get the created dataset
            dataset = client.get_dataset(result['dataset_id'])
            print("\nFetching dataset details...")
            details = dataset.get_details()
            print(f"Dataset Details: {details}")
            
        except ValueError as e:
            print(f"Validation error: {str(e)}")
        except requests.exceptions.RequestException as e:
            print(f"Error creating dataset: {str(e)}")

        # Get all available datasets
        print("\nFetching all datasets...")
        all_datasets = client.get_all_datasets()
        print(f"Found {len(all_datasets)} datasets")

        # Get sample datasets
        print("\nFetching sample datasets...")
        sample_datasets = client.get_sample_datasets()
        print(f"Found {len(sample_datasets)} sample datasets:")
        for dataset in sample_datasets:
            print(f"- {dataset['display_name']} (ID: {dataset['dataset_id']})")

        # Example: Working with a specific dataset
        if sample_datasets:
            print("\nWorking with a sample dataset...")
            sample_dataset = client.get_dataset(sample_datasets[0]['dataset_id'])
            
            # Get dataset details
            print("\nFetching dataset details...")
            details = sample_dataset.get_details()
            print(f"Dataset Name: {details.get('name', 'N/A')}")
            print(f"Dataset Type: {details.get('type', 'N/A')}")
            
            # Get dataset statistics
            print("\nFetching dataset statistics...")
            stats = sample_dataset.get_stats()
            print(f"Statistics: {stats}")
            
            # Explore dataset
            print("\nExploring dataset...")
            exploration = sample_dataset.explore()
            print(f"Exploration results: {exploration}")

        # Example: Working with a custom dataset
        custom_dataset_id = "3972b3fc-1809-11ef-bb76-064432e0d220"  # Replace with your dataset ID
        print(f"\nWorking with custom dataset (ID: {custom_dataset_id})...")
        custom_dataset = client.get_dataset(custom_dataset_id)
        
        try:
            details = custom_dataset.get_details()
            print(f"Custom Dataset Details: {details}")
        except requests.exceptions.RequestException as e:
            print(f"Error accessing custom dataset: {str(e)}")
            
    except requests.exceptions.RequestException as e:
        print(f"\nError: {str(e)}")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")

if __name__ == "__main__":
    main()