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
            headers=self.client._get_headers_no_jwt()
        )
        response.raise_for_status()
        return response.json()
    
    def export(self) -> dict:
        """Export this dataset in JSON format"""
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

        # Explore specific dataset
        dataset_id = "9178adde-31c8-11f0-93d6-4e7b67d53dad"
        print(f"\nExploring dataset with ID: {dataset_id}")
        
        dataset = client.get_dataset(dataset_id)
        print("\nFetching dataset details...")
        details = dataset.get_details()
        print(f"Dataset Details: {details}")
        
        print("\nExploring dataset previews...")
        df = dataset.explore()
        
        if not df.empty:
            # Display DataFrame information
            print("\n=== Dataset Previews ===")
            print(f"\nNumber of previews: {len(df)}")
            print(f"Number of columns: {len(df.columns)}")
            
            # Select and display relevant columns
            relevant_columns = ['file_name', 'image_uri', 'media_uri', 'media_thumb_uri', 'relevance_score']
            print("\nPreview information:")
            print(df[relevant_columns].head())
            
            # Save to CSV if needed
            csv_filename = f"dataset_{dataset_id}_previews.csv"
            df.to_csv(csv_filename, index=False)
            print(f"\nSaved previews to {csv_filename}")
        else:
            print("\nNo previews found in the dataset")
            
        # Export specific dataset
        print(f"\nExporting dataset with ID: {dataset_id}")
        export_data = dataset.export()
        print("\n=== Exported Data (truncated to first 1000 characters) ===")
        export_str = str(export_data)
        print(export_str[:1000] + ("..." if len(export_str) > 1000 else ""))
        
        # Optionally, save to a file
        export_filename = f"dataset_{dataset_id}_export.json"
        with open(export_filename, "w") as f:
            import json
            json.dump(export_data, f, indent=2)
        print(f"\nExported data saved to {export_filename}")
        
    except requests.exceptions.RequestException as e:
        print(f"\nError: {str(e)}")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")

if __name__ == "__main__":
    main()