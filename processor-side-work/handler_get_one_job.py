import json
import os
import urllib.request
import urllib.error
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Get API URL from environment variable
API_URL = os.environ.get('API_URL', 'https://yknlfsjyye.execute-api.us-east-1.amazonaws.com/dev/prepare-job')

def get_one_job(event, context):
    """
    Lambda function that calls the prepare-job API and returns the response.
    This function fetches a job from the API and passes it through as the response.
    """
    logger.info(f"Fetching job from {API_URL}")
    
    try:
        # Make request to the API
        with urllib.request.urlopen(API_URL) as response:
            response_data = response.read().decode('utf-8')
            logger.info(f"Received response from API")
            
            # Parse the response as JSON
            job_data = json.loads(response_data)
            
            # Log job ID if available
            if isinstance(job_data, dict) and 'body' in job_data:
                body = job_data['body']
                if isinstance(body, str):
                    body = json.loads(body)
                if isinstance(body, dict) and 'job_id' in body:
                    logger.info(f"Retrieved job with ID: {body['job_id']}")
            
            # Return the API response directly
            return {
                "statusCode": 200,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": response_data
            }
            
    except urllib.error.HTTPError as e:
        logger.error(f"HTTP Error: {e.code} - {e.reason}")
        return {
            "statusCode": e.code,
            "body": json.dumps({
                "error": f"HTTP Error: {e.reason}"
            })
        }
    except urllib.error.URLError as e:
        logger.error(f"URL Error: {e.reason}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": f"URL Error: {e.reason}"
            })
        }
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": f"Unexpected error: {str(e)}"
            })
        }