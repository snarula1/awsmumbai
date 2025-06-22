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
s3_bucket = "myrootfolder"
CALLBACK_URL = "https://wbszcff9rl.execute-api.us-east-1.amazonaws.com/dev/get-upload-url/{job_id}"
HARDCODED_KEYS = [
    "smallfilezip/file1.pdf",
    "smallfilezip/file2.pdf",
    "smallfilezip/file3.pdf",
    "smallfilezip/file4.pdf"
]

def generate_presigned_url(bucket, key, expiration=36000):
    return s3.generate_presigned_url(
        'get_object',
        Params={'Bucket': bucket, 'Key': key},
        ExpiresIn=expiration
    )

def lambda_handler(event, context):
    job_id = str(uuid.uuid4())
    file_object_list = []

    for key in HARDCODED_KEYS:
        presigned_url = generate_presigned_url(s3_bucket, key)
        file_name = os.path.basename(key)
        file_object_list.append({
            "file_name": file_name,
            "presigned_url": presigned_url
        })

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
        "job_file_download_url": job_file_presigned_url
    }
    
    logger.info(f"Job created with ID: {job_id}, S3 Key: {job_file_key}")
    logger.info(f"Response JSON: {json.dumps(json_object)}")
    
    return {
        "statusCode": 200,
        "body": json.dumps(json_object)
    }