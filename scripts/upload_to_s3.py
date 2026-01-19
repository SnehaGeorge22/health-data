# save as: upload_to_s3.py

import boto3
import os
from pathlib import Path

# CONFIGURE THESE
BUCKET_NAME = 'healthcare-pipeline-raw-sg'  # ← CHANGE THIS
AWS_REGION = 'us-east-1'  # ← CHANGE IF NEEDED

s3_client = boto3.client('s3', region_name=AWS_REGION)

def upload_file(file_path, s3_key):
    """Upload a single file to S3"""
    try:
        print(f"Uploading: {file_path.name} → s3://{BUCKET_NAME}/{s3_key}")
        
        s3_client.upload_file(
            str(file_path),
            BUCKET_NAME,
            s3_key,
            ExtraArgs={'ServerSideEncryption': 'AES256'}
        )
        
        print(f"✓ Uploaded successfully")
        return True
        
    except Exception as e:
        print(f"✗ Upload failed: {e}")
        return False

def upload_directory(local_dir, s3_prefix):
    """Upload all CSV files from directory to S3"""
    local_path = Path(local_dir)
    
    if not local_path.exists():
        print(f"✗ Directory not found: {local_dir}")
        return
    
    # Find all CSV files
    csv_files = list(local_path.rglob('*.csv'))
    
    print(f"\nFound {len(csv_files)} CSV files in {local_dir}")
    
    for csv_file in csv_files:
        # Create S3 key
        s3_key = f"{s3_prefix}/{csv_file.name}"
        
        # Upload
        upload_file(csv_file, s3_key)

def main():
    print(f"{'='*60}")
    print(f"Uploading CMS data to S3 bucket: {BUCKET_NAME}")
    print(f"{'='*60}\n")
    
    # Define mappings
    mappings = {
        'cms_data_downloads/beneficiary/extracted': 'landing/beneficiary',
        'cms_data_downloads/inpatient/extracted': 'landing/inpatient',
        'cms_data_downloads/outpatient/extracted': 'landing/outpatient',
        'cms_data_downloads/carrier/extracted': 'landing/carrier',
        'cms_data_downloads/prescription/extracted': 'landing/prescription'
    }
    
    for local_dir, s3_prefix in mappings.items():
        print(f"\n{'='*60}")
        print(f"Processing: {local_dir}")
        print(f"{'='*60}")
        upload_directory(local_dir, s3_prefix)
    
    print(f"\n{'='*60}")
    print("✓ All uploads complete!")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()