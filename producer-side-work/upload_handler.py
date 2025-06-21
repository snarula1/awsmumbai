import json
import boto3
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
S3_BUCKET = os.environ.get('S3_BUCKET_NAME', 'myrootfolder')
UPLOAD_FOLDER = 'zip_processed_by_processor'

def lambda_handler(event, context):
    """
    Generate a presigned URL for uploading a zip file to S3
    """
    # Log the entire event for debugging
    logger.info(f"Received event: {json.dumps(event)}")
    
    # For lambda-proxy integration, the job_id will be in queryStringParameters
    job_id = None
    if event and isinstance(event, dict):
        if 'queryStringParameters' in event and event['queryStringParameters'] and event['queryStringParameters'].get('job_id'):
            job_id = event['queryStringParameters'].get('job_id')
            logger.info(f"Found job_id in queryStringParameters: {job_id}")
    
    # Hardcode a job_id for testing if none is provided
    if not job_id:
        job_id = "test-job-id"
        logger.warning(f"No job_id found in request, using default: {job_id}")
    
    logger.info(f"Using job_id: {job_id}")
    
    try:
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Define the S3 key for the zip file with nested folder structure
        s3_key = f"{UPLOAD_FOLDER}/{job_id}/{job_id}.zip"
        
        # Generate presigned URL for uploading
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': S3_BUCKET,
                'Key': s3_key,
                'ContentType': 'application/zip'
            },
            ExpiresIn=36000  # URL valid for 10 hours
        )
        
        logger.info(f"Generated presigned URL for S3 key: {s3_key}")
        
        # Return the presigned URL
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({
                "job_id": job_id,
                "upload_url": presigned_url,
                "s3_key": s3_key,
                "expires_in": "10 hours"
            })
        }
        
    except Exception as e:
        logger.error(f"Error generating presigned URL: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"message": f"Error generating presigned URL: {str(e)}"})
        }