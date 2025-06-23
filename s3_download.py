#!/usr/bin/env python3
"""
S3 Download Script
Downloads CSV files from S3 bucket to local storage
"""

import boto3
import os
import logging
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

def download_from_s3(bucket_name, s3_key, local_path):
    """
    Download a file from S3 to local storage
    """
    try:
        s3_client = boto3.client('s3')
        
        logger.info(f"Downloading {s3_key} from S3...")
        s3_client.download_file(bucket_name, s3_key, local_path)
        
        # Check if file was downloaded successfully
        if os.path.exists(local_path):
            file_size = os.path.getsize(local_path)
            logger.info(f"✅ Successfully downloaded {s3_key} ({file_size} bytes)")
            return True
        else:
            logger.error(f"❌ Error: File {local_path} was not created")
            return False
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchKey':
            logger.error(f"❌ Error: File {s3_key} not found in bucket {bucket_name}")
        elif error_code == 'NoSuchBucket':
            logger.error(f"❌ Error: Bucket {bucket_name} does not exist")
        else:
            logger.error(f"❌ Error downloading {s3_key}: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error downloading {s3_key}: {e}")
        return False

def main():
    """
    Download both CSV files from S3
    """
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    if not bucket_name:
        logger.error("❌ Error: S3_BUCKET_NAME not found in .env file")
        exit(1)
    
    # Create ppp_csvs directory if it doesn't exist
    csv_dir = "ppp_csvs"
    if not os.path.exists(csv_dir):
        logger.info(f"Creating directory: {csv_dir}")
        os.makedirs(csv_dir)
    
    # Files to download
    files_to_download = [
        ('ppp.csv', 'ppp_csvs/ppp.csv'),
        ('ppp_subset.csv', 'ppp_csvs/ppp_subset.csv')
    ]
    
    logger.info(f"=== Downloading files from S3 bucket: {bucket_name} ===")
    
    success_count = 0
    for s3_key, local_path in files_to_download:
        if download_from_s3(bucket_name, s3_key, local_path):
            success_count += 1
    
    if success_count == len(files_to_download):
        logger.info(f"✅ Successfully downloaded all {success_count} files!")
        return True
    else:
        logger.error(f"❌ Only {success_count}/{len(files_to_download)} files downloaded successfully")
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1) 