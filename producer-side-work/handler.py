import boto3
import uuid
import json
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

s3 = boto3.client('s3')

# Constants
s3_bucket = os.environ.get('S3_BUCKET_NAME', 'myrootfolder')
CALLBACK_URL = "https://yknlfsjyye.execute-api.us-east-1.amazonaws.com/dev/get-upload-url?job_id={job_id}"
# HARDCODED_KEYS = [
#     "smallfilezip/file1.pdf",
#     "smallfilezip/file2.pdf",
#     "smallfilezip/file3.pdf",
#     "smallfilezip/file4.pdf"
# ]

HARDCODED_KEYS = [
    "largefileziptest/my1gbfile1.pdf",
    "largefileziptest/my1gbfile2.pdf",
    "largefileziptest/my1gbfile3.pdf",
    "largefileziptest/my1gbfile4.pdf",
    "largefileziptest/my1gbfile5.pdf",
    "largefileziptest/my1gbfile6.pdf",
    "largefileziptest/my1gbfile7.pdf",
    "largefileziptest/my1gbfile8.pdf",
    "largefileziptest/my1gbfile9.pdf",
    "largefileziptest/my1gbfile10.pdf"
]
def get_large_hardcoded_keys():
    """Generate keys for 10 large files (1GB to 10GB)"""
    return [f"largefileziptest/my1gbfile{n}.pdf" for n in range(1, 11)]

# Large file keys for testing
LARGE_HARDCODED_KEYS = get_large_hardcoded_keys()
# Uncomment the line below to use large files by default
# HARDCODED_KEYS = LARGE_HARDCODED_KEYS
def validate_s3_object_exists(bucket, key):
    """Check if an object exists in S3 before generating a presigned URL"""
    try:
        # Use head_object to check if the object exists without downloading it
        response = s3.head_object(Bucket=bucket, Key=key)
        
        # Log object metadata for debugging
        size_mb = response.get('ContentLength', 0) / (1024 * 1024)
        logger.info(f"Validated S3 object {bucket}/{key} exists. Size: {size_mb:.2f} MB")
        return True
    except s3.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        if error_code == '404':
            logger.error(f"S3 object {bucket}/{key} does not exist (404)")
        else:
            logger.error(f"Error validating S3 object {bucket}/{key}: {error_code} - {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error validating S3 object {bucket}/{key}: {str(e)}")
        return False

def generate_presigned_url(bucket, key, expiration=36000):
    """Generate a presigned URL after validating the object exists"""
    # First validate that the object exists
    if not validate_s3_object_exists(bucket, key):
        logger.warning(f"Object {bucket}/{key} does not exist, presigned URL may not work")
        
    # Generate the presigned URL
    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        logger.error(f"Error generating presigned URL for {bucket}/{key}: {str(e)}")
        return None

def lambda_handler(event, context):
    job_id = str(uuid.uuid4())
    file_object_list = []
    
    # Check if we should use large files
    use_large_files = event.get('queryStringParameters', {}).get('large') == 'true'
    keys_to_use = LARGE_HARDCODED_KEYS if use_large_files else HARDCODED_KEYS
    
    logger.info(f"Using {'large' if use_large_files else 'small'} files for job {job_id}")

    errors = []
    for key in keys_to_use:
        presigned_url = generate_presigned_url(s3_bucket, key)
        
        if not presigned_url:
            error_msg = f"Failed to generate presigned URL for {key}"
            logger.error(error_msg)
            errors.append({"file": key, "error": error_msg})
            continue
            
        # Validate URL format
        if not presigned_url.startswith('https://'):
            error_msg = f"Generated URL doesn't look valid: {presigned_url[:50]}..."
            logger.warning(error_msg)
            errors.append({"file": key, "error": error_msg})
            
        file_name = os.path.basename(key)
        file_object_list.append({
            "file_name": file_name,
            "presigned_url": presigned_url
        })
        
        logger.info(f"Added file {file_name} with presigned URL (first 50 chars): {presigned_url[:50]}...")

    job_request = {
        "JobRequest": {
            "job_id": job_id,
            "file_object": file_object_list,
            "call_url_when_done_with_job_id": CALLBACK_URL.format(job_id=job_id)
        }
    }

    # Check if folder exists and create it if needed
    try:
        # List objects with the prefix to check if folder exists
        response = s3.list_objects_v2(
            Bucket=s3_bucket,
            Prefix="my_jobs_to_send/",
            MaxKeys=1
        )
        
        # If folder doesn't exist (no objects with that prefix)
        if 'Contents' not in response:
            # Create an empty object with the folder name (S3 convention for creating folders)
            s3.put_object(
                Bucket=s3_bucket,
                Key="my_jobs_to_send/",
                Body=""
            )
            logger.info("Created my_jobs_to_send/ folder in S3")
    except Exception as e:
        logger.error(f"Error checking/creating folder: {str(e)}")

    job_file_key = f"my_jobs_to_send/{job_id}.json"

    # Store the job JSON file in S3
    s3.put_object(
        Bucket=s3_bucket,
        Key=job_file_key,
        Body=json.dumps(job_request),
        ContentType='application/json'
    )

    # Generate 10-hour presigned URL for job file
    job_file_presigned_url = generate_presigned_url(
        bucket=s3_bucket,
        key=job_file_key,
        expiration=36000  # 10 hours in seconds
    )
    
    json_object = {
        "message": "Job created successfully.",
        "job_id": job_id,
        "job_file_s3_key": job_file_key,
        "job_file_download_url": job_file_presigned_url,
        "files_count": len(file_object_list)
    }
    
    # Add errors to the response if any occurred
    if errors:
        json_object["errors"] = errors
        json_object["message"] = f"Job created with {len(errors)} errors. Some files may not be accessible."
    
    logger.info(f"Job created with ID: {job_id}, S3 Key: {job_file_key}")
    logger.info(f"Response JSON: {json.dumps(json_object)}")
    
    return {
        "statusCode": 200,
        "body": json.dumps(json_object)
    }