#!/usr/bin/env python3
"""
S3 Connection Test Script
Tests connectivity to S3 bucket and lists available files
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

def test_s3_connection(bucket_name):
    """
    Test connection to S3 bucket and list contents
    """
    try:
        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        logger.info(f"Testing connection to S3 bucket: {bucket_name}")
        
        # Test if we can list objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=10)
        
        if 'Contents' in response:
            logger.info("✅ Successfully connected to S3 bucket!")
            logger.info(f"Found {len(response['Contents'])} objects in bucket:")
            
            for obj in response['Contents']:
                logger.info(f"  - {obj['Key']} ({obj['Size']} bytes)")
                
            return True
        else:
            logger.warning("⚠️  Connected to bucket but no objects found")
            return True
            
    except NoCredentialsError:
        logger.error("❌ Error: AWS credentials not found")
        logger.error("Please ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set in .env file")
        return False
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            logger.error(f"❌ Error: Bucket '{bucket_name}' does not exist")
        elif error_code == 'AccessDenied':
            logger.error(f"❌ Error: Access denied to bucket '{bucket_name}'")
        elif error_code == 'InvalidAccessKeyId':
            logger.error(f"❌ Error: Invalid AWS Access Key ID")
            logger.error("Please check your AWS_ACCESS_KEY_ID in .env file")
        else:
            logger.error(f"❌ Error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

if __name__ == "__main__":
    bucket_name = os.getenv('S3_BUCKET_NAME')
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    if not bucket_name:
        logger.error("❌ Error: S3_BUCKET_NAME not found in .env file")
        exit(1)
    
    logger.info("=== S3 Connection Test ===")
    logger.info(f"Using bucket: {bucket_name}")
    logger.info(f"AWS Region: {region}")
    
    success = test_s3_connection(bucket_name)
    
    if success:
        logger.info("✅ S3 connection test passed!")
    else:
        logger.error("❌ S3 connection test failed!")
        exit(1) 