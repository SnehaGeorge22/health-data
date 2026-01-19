"""
Lambda Function: Data Validation (Memory Optimized)
Purpose: Validate S3 files before processing - streams data to handle large files
Trigger: Step Functions workflow
"""

import json
import boto3
import csv
from datetime import datetime

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

# Configuration
EXPECTED_SCHEMAS = {
    'beneficiary': [
        'DESYNPUF_ID', 'BENE_BIRTH_DT', 'BENE_DEATH_DT', 'BENE_SEX_IDENT_CD',
        'BENE_RACE_CD', 'BENE_ESRD_IND', 'SP_STATE_CODE', 'BENE_COUNTY_CD'
    ],
    'inpatient': [
        'DESYNPUF_ID', 'CLM_ID', 'SEGMENT', 'CLM_FROM_DT', 'CLM_THRU_DT',
        'PRVDR_NUM', 'CLM_PMT_AMT', 'NCH_PRMRY_PYR_CLM_PD_AMT'
    ],
    'outpatient': [
        'DESYNPUF_ID', 'CLM_ID', 'SEGMENT', 'CLM_FROM_DT', 'CLM_THRU_DT',
        'PRVDR_NUM', 'CLM_PMT_AMT'
    ],
    'carrier': [
        'DESYNPUF_ID', 'CLM_ID', 'CLM_FROM_DT', 'CLM_THRU_DT',
        'ICD9_DGNS_CD_1', 'PRF_PHYSN_NPI'
    ],
    'prescription': [
        'DESYNPUF_ID', 'PDE_ID', 'SRVC_DT', 'PROD_SRVC_ID',
        'QTY_DSPNSD_NUM', 'DAYS_SUPLY_NUM', 'TOT_RX_CST_AMT'
    ]
}

MIN_ROWS = {
    'beneficiary': 100,
    'inpatient': 50,
    'outpatient': 50,
    'carrier': 100,
    'prescription': 50
}

# Maximum rows to sample for large files (to avoid memory issues)
MAX_SAMPLE_ROWS = 10000

def lambda_handler(event, context):
    """
    Main Lambda handler - memory optimized for large files
    
    Event structure:
    {
        "bucket": "healthcare-pipeline-raw-yourname",
        "prefix": "landing/beneficiary/",
        "data_type": "beneficiary"
    }
    """
    
    try:
        bucket = event.get('bucket')
        prefix = event.get('prefix')
        data_type = event.get('data_type')
        
        print(f"Validating {data_type} data in s3://{bucket}/{prefix}")
        
        # List files in the prefix
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' not in response:
            raise ValueError(f"No files found in s3://{bucket}/{prefix}")
        
        # Get CSV files only
        csv_files = [obj['Key'] for obj in response['Contents'] 
                     if obj['Key'].endswith('.csv')]
        
        if not csv_files:
            raise ValueError(f"No CSV files found in s3://{bucket}/{prefix}")
        
        print(f"Found {len(csv_files)} CSV files")
        
        validation_results = []
        
        # Validate each file (use streaming for large files)
        for file_key in csv_files:
            print(f"\nValidating: {file_key}")
            
            result = validate_file_streaming(bucket, file_key, data_type)
            validation_results.append(result)
            
            if not result['valid']:
                print(f"✗ Validation failed: {result['errors']}")
            else:
                print(f"✓ Validation passed: {result['row_count']} rows")
        
        # Overall validation status
        all_valid = all(r['valid'] for r in validation_results)
        
        response_payload = {
            'statusCode': 200 if all_valid else 400,
            'validation_status': 'PASSED' if all_valid else 'FAILED',
            'data_type': data_type,
            'files_validated': len(csv_files),
            'results': validation_results,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Send SNS notification if validation failed
        if not all_valid:
            send_failure_notification(data_type, validation_results)
        
        return response_payload
        
    except Exception as e:
        print(f"Error in validation: {str(e)}")
        
        return {
            'statusCode': 500,
            'validation_status': 'ERROR',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }


def validate_file_streaming(bucket, file_key, data_type):
    """
    Validate a CSV file using streaming to handle large files
    Memory efficient - doesn't load entire file into memory
    """
    errors = []
    warnings = []
    
    try:
        # Get file metadata
        head_response = s3_client.head_object(Bucket=bucket, Key=file_key)
        file_size = head_response['ContentLength']
        
        print(f"File size: {file_size / (1024*1024):.2f} MB")
        
        # Determine if we should sample (for very large files)
        is_large_file = file_size > 50 * 1024 * 1024  # > 50 MB
        
        # Stream the file
        response = s3_client.get_object(Bucket=bucket, Key=file_key)
        
        # Read file in chunks
        lines = response['Body'].iter_lines()
        
        # Get header
        header_line = next(lines).decode('utf-8')
        headers = [h.strip() for h in header_line.split(',')]
        
        # Validate schema
        expected_columns = EXPECTED_SCHEMAS.get(data_type, [])
        
        if expected_columns:
            # Check if expected columns exist
            missing_columns = [col for col in expected_columns 
                             if col not in headers]
            
            if missing_columns:
                errors.append(f"Missing columns: {missing_columns}")
        
        # Count rows and check data quality (sample if large file)
        row_count = 0
        null_counts = {col: 0 for col in headers}
        sample_row_count = 0
        
        for line in lines:
            row_count += 1
            
            # For large files, only validate a sample
            if is_large_file and sample_row_count >= MAX_SAMPLE_ROWS:
                # Just count remaining rows without validation
                continue
            
            try:
                row = line.decode('utf-8').split(',')
                
                # Count nulls in each column (only for sample)
                if not is_large_file or sample_row_count < MAX_SAMPLE_ROWS:
                    for i, value in enumerate(row):
                        if i < len(headers):
                            col = headers[i]
                            if not value or value.strip() == '':
                                null_counts[col] += 1
                    
                    sample_row_count += 1
                    
            except Exception as row_error:
                # Skip malformed rows but log them
                if len(errors) < 10:  # Limit error messages
                    errors.append(f"Malformed row at line {row_count}: {str(row_error)}")
        
        if is_large_file:
            warnings.append(f"Large file - validated sample of {sample_row_count} rows from {row_count} total")
        
        # Check minimum row count
        min_rows = MIN_ROWS.get(data_type, 1)
        if row_count < min_rows:
            errors.append(f"Insufficient rows: {row_count} < {min_rows}")
        
        # Check for excessive nulls in critical columns
        critical_columns = ['DESYNPUF_ID']
        
        validation_row_count = sample_row_count if is_large_file else row_count
        
        for col in critical_columns:
            if col in null_counts and validation_row_count > 0:
                null_percentage = (null_counts[col] / validation_row_count) * 100
                if null_percentage > 0:
                    errors.append(f"{col} has {null_percentage:.1f}% null values in sample")
        
        # File size check
        if file_size < 1000:  # Less than 1KB is suspicious
            warnings.append(f"File size is very small: {file_size} bytes")
        
        return {
            'file': file_key,
            'valid': len(errors) == 0,
            'row_count': row_count,
            'sampled_rows': sample_row_count if is_large_file else row_count,
            'column_count': len(headers),
            'columns': headers[:10],  # Only return first 10 column names
            'file_size_bytes': file_size,
            'file_size_mb': round(file_size / (1024*1024), 2),
            'is_large_file': is_large_file,
            'errors': errors,
            'warnings': warnings,
            'null_counts': {k: v for k, v in list(null_counts.items())[:10] if v > 0}  # Limit output
        }
        
    except Exception as e:
        return {
            'file': file_key,
            'valid': False,
            'errors': [f"Exception during validation: {str(e)}"],
            'warnings': []
        }


def send_failure_notification(data_type, validation_results):
    """
    Send SNS notification when validation fails
    """
    try:
        sns_topic_arn = 'arn:aws:sns:us-east-1:123456789012:healthcare-pipeline-notifications'
        
        failed_files = [r for r in validation_results if not r['valid']]
        
        message = f"""
Healthcare Data Pipeline - Validation FAILED

Data Type: {data_type}
Timestamp: {datetime.utcnow().isoformat()}

Failed Files: {len(failed_files)}

Details:
"""
        
        for result in failed_files[:5]:  # Limit to first 5 failures
            message += f"\nFile: {result['file']}\n"
            message += f"Errors: {', '.join(result['errors'][:3])}\n"  # Limit errors
        
        if len(failed_files) > 5:
            message += f"\n... and {len(failed_files) - 5} more files failed"
        
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject=f'Healthcare Pipeline Validation Failed - {data_type}',
            Message=message
        )
        
        print("SNS notification sent")
        
    except Exception as e:
        print(f"Failed to send SNS notification: {str(e)}")