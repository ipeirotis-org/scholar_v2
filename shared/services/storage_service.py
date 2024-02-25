from google.cloud import storage

from ..config import Config

class StorageService:
    def __init__(self):
        self.storage_client = storage.Client(project=Config.PROJECT_ID)
        self.bucket_name = Config.BUCKET_NAME
        self.bucket = self.storage_client.bucket(self.bucket_name)
       

    def upload_csv_to_gcs(self, df, destination_blob_name):
        """Uploads the CSV content to Google Cloud Storage."""
        
        blob = self.bucket.blob(destination_blob_name)
        # Convert DataFrame to a CSV string
        csv_string = df.to_csv(index=False)
        blob.upload_from_string(csv_string, content_type='text/csv')
        
    
    def generate_signed_url(self, blob_name):
        """Generates a signed URL for the blob."""

        blob = self.bucket.blob(blob_name)
        
        url = blob.generate_signed_url(version="v4",
                                       expiration=datetime.timedelta(minutes=15),  # URL expires in 15 minutes
                                       method="GET")
        return url


