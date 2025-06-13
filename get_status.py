from uuid import UUID
import requests
from datetime import datetime, timezone, timedelta
import jwt
import os
from dotenv import load_dotenv

# Load environment variables


class VisualLayerClient:
    def __init__(self, api_key: str, api_secret: str):
        # Use staging environment
        self.base_url = "https://app.staging-visual-layer.link/api/v1"
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
    
    def get_dataset_stats(self, dataset_id: UUID) -> dict:
        """Get statistics for a specific dataset"""
        response = self.session.get(
            f"{self.base_url}/dataset/{dataset_id}/stats",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
    
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
    
    def get_dataset_stats(self, dataset_id: str) -> dict:
        """Get statistics for a specific dataset"""
        response = self.session.get(
            f"{self.base_url}/dataset/{dataset_id}/stats",
            headers=self._get_headers()
        )
        response.raise_for_status()
        return response.json()
   
    def get_dataset_by_id(self, dataset_id: str) -> dict:
        """Get a specific dataset"""
        response = self.session.get(
            f"{self.base_url}/dataset/{dataset_id}",
            headers=self._get_headers_no_jwt()
        )
        response.raise_for_status()
        return response.json()
    

# Usage example:
def main():
    load_dotenv()
    print(os.getenv('VISUAL_LAYER_API_KEY'))
    print(os.getenv('VISUAL_LAYER_API_SECRET'))

    # Get API credentials from environment
    API_KEY = os.getenv('VISUAL_LAYER_API_KEY')
    API_SECRET = os.getenv('VISUAL_LAYER_API_SECRET')
    
    if not API_KEY or not API_SECRET:
        print("Error: API credentials not found in environment variables")
        print("Please make sure VISUAL_LAYER_API_KEY and VISUAL_LAYER_API_SECRET are set in your .env file")
        return
    
    print("Initializing client...")
    client = VisualLayerClient(API_KEY, API_SECRET)
    
    try:
        print("Checking health...")
        client.healthcheck()
        print("Health check passed")
        
        print("Getting all datasets...")
        all_datasets = client.get_all_datasets()
        print("Got all datasets")
#        for dataset in all_datasets:
#            stats = client.get_dataset_by_id(dataset['id'])
#            print(stats)

        # Get sample datasets
        print("\nGetting sample datasets...")
        sample_datasets = client.get_sample_datasets()
        print("Available sample datasets:")
        for dataset in sample_datasets:
            print(f"ID: {dataset['dataset_id']}, Name: {dataset['display_name']}")
        
        # Get stats for a known test dataset
        test_dataset_id = UUID("874cd684-d097-11ee-8ff9-c25b68c514c3")  # Known test dataset ID
        print(f"\nGetting stats for dataset: {test_dataset_id}")
        stats = client.get_dataset_stats(test_dataset_id)
        
        print("\nDataset Stats:")
        print(f"Dataset: {stats['dataset']}")
        if stats.get('redirect_url'):
            print(f"Redirect URL: {stats['redirect_url']}")
            
    except requests.exceptions.RequestException as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()