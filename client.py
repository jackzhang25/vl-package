from uuid import UUID
import requests
from datetime import datetime, timezone, timedelta
import jwt
import os
from dotenv import load_dotenv
import pandas as pd

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
            headers=self.client._get_headers()
        )
        response.raise_for_status()
        return response.json()

    def explore(self) -> pd.DataFrame:
        """Explore this dataset and return previews as a DataFrame"""
        response = self.client.session.get(
            f"{self.base_url}/explore/{self.dataset_id}",
            headers=self.client._get_headers()
        )
        response.raise_for_status()
        data = response.json()
        
        # Extract just the previews from the first cluster
        if data.get('clusters') and len(data['clusters']) > 0:
            previews = data['clusters'][0].get('previews', [])
            # Convert previews to DataFrame
            df = pd.DataFrame(previews)
            return df
        else:
            return pd.DataFrame()  # Return empty DataFrame if no previews found

    def delete(self) -> dict:
        """Delete this dataset"""
        response = self.client.session.delete(
            f"{self.base_url}/dataset/{self.dataset_id}",
            headers=self.client._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
    def export(self) -> dict:
        """Export this dataset in JSON format"""
        # Check if dataset is ready before exporting
        status = self.get_status()
        if status not in ['READY', 'completed']:
            raise RuntimeError(f"Cannot export dataset {self.dataset_id}. Current status: {status}. Dataset must be 'ready' or 'completed' to export.")
        
        url = f"{self.base_url}/dataset/{self.dataset_id}/export"
        params = {'export_format': 'json'}
        headers = {
            **self.client._get_headers()
        }
        response = self.client.session.get(
            url,
            params=params,
            headers=headers
        )
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
            if status not in ['READY', 'completed']:
                print(f"Warning: Dataset {self.dataset_id} is not ready for export. Current status: {status}")
                return pd.DataFrame()
            
            # Export the dataset
            export_data = self.export()
            
            # Extract media_items from the export data
            if 'media_items' in export_data:
                media_items = export_data['media_items']
                
                # Remove metadata_items from each media item if it exists
                cleaned_media_items = []
                for item in media_items:
                    # Create a copy of the item without metadata_items
                    cleaned_item = {k: v for k, v in item.items() if k != 'metadata_items'}
                    cleaned_media_items.append(cleaned_item)
                
                # Convert to DataFrame
                df = pd.DataFrame(cleaned_media_items)
                return df
            else:
                print("No media_items found in export data")
                return pd.DataFrame()
                
        except Exception as e:
            print(f"Error exporting dataset {self.dataset_id}: {str(e)}")
            return pd.DataFrame()
    
    def get_status(self) -> dict:
        return self.get_details()['status']

class VisualLayerClient:
    def __init__(self, api_key: str, api_secret: str):
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
    #limits on number of datasets?
    # return a datafram instead of json
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
            'dataset_name': dataset_name,
            'vl_dataset_id': '',
            'bucket_path': s3_bucket_path,
            'uploaded_filename': '',
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

        # Test creating dataset from S3 bucket
        print("\n=== Testing S3 Bucket Dataset Creation ===")
        s3_bucket_path = "pokemondataset/images/"
        dataset_name = "pokemon_test_dataset"
        
        print(f"Creating dataset from S3 bucket: {s3_bucket_path}")
        print(f"Dataset name: {dataset_name}")
        
        result = client.create_dataset_from_s3_bucket(s3_bucket_path, dataset_name)
        print(f"\nDataset creation result: {result}")
        
        # If successful, get the dataset ID and test other operations
        if result.get('dataset_id'):
            dataset_id = result['dataset_id']
            print(f"\nCreated dataset with ID: {dataset_id}")
            
            # Get dataset details
            dataset = client.get_dataset(dataset_id)
            details = dataset.get_details()
            print(f"\nDataset details: {details}")
            
            # Get dataset stats
            stats = dataset.get_status()
            print(f"\nDataset stats: {stats}")

        # Test the new export function
        dataset_id = "9178adde-31c8-11f0-93d6-4e7b67d53dad"
        print(f"\nExporting dataset {dataset_id} to DataFrame...")
        
        dataset = client.get_dataset(dataset_id)
        df = dataset.export_to_dataframe()
        
        if not df.empty:
            print(f"\n=== Exported Dataset DataFrame ===")
            print(f"Number of media items: {len(df)}")
            print(f"Number of columns: {len(df.columns)}")
            print(f"\nColumns: {list(df.columns)}")
            print(f"\nFirst few rows:")
            print(df.head())
            
            # Save to CSV
            #csv_filename = f"dataset_{dataset_id}_media_items.csv"
            #df.to_csv(csv_filename, index=False)
            #print(f"\nSaved media items to {csv_filename}")
        else:
            print("No data to export")
        print(df.iloc[0]['file_name'])
    except requests.exceptions.RequestException as e:
        print(f"\nError: {str(e)}")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")

if __name__ == "__main__":
    main()