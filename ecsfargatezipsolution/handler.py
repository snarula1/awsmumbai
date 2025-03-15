import json
import requests
import os
import shutil
import boto3
import datetime
import uuid 

s3_client = boto3.client("s3")
sqs_client = boto3.client("sqs")
lambda_client = boto3.client("lambda")

S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "your-bucket-name")  # Change to actual bucket
S3_FOLDER_NAME = os.getenv("S3_FOLDER_NAME", "your-folder-name/")  # Change to actual folder (must end with '/')
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "your-sqs-queue-url")  # SQS Queue URL
LAMBDA_FUNCTION_URL = os.getenv("LAMBDA_FUNCTION_URL", "https://a98nrda34b.execute-api.us-east-1.amazonaws.com/dev/")  # External URL of your Lambda
EXTERNAL_LAMBDA_ENDPOINT = os.getenv("EXTERNAL_LAMBDA_ENDPOINT", "https://your-external-lambda-endpoint.com")  # External Lambda HTTP endpoint
OUTPUT_JSON_FILE_PATH = "jobs/{job_id}.json"  # Use dynamic job ID for the file name


s3_client = boto3.client("s3")

def lambda_handler(event, json_file_name, bucket_name, context=None):
    # Extract folder name from JSON file name (without extension)
    folder_name = os.path.splitext(json_file_name)[0]
    download_dir = os.path.join("/tmp", folder_name)  # Use /tmp in AWS Lambda
    os.makedirs(download_dir, exist_ok=True)  # Ensure it exists

    file_count = 0  # Counter for downloaded files

    # Iterate through the presigned URLs and download files
    for file_info in event.get("presigned_urls", []):
        file_name = file_info.get("file_name")
        url = file_info.get("url")

        if file_name and url:
            file_path = os.path.join(download_dir, file_name)
            response = requests.get(url)
            if response.status_code == 200:
                with open(file_path, "wb") as file:
                    file.write(response.content)
                print(f"Downloaded: {file_path}")
                file_count += 1
            else:
                print(f"Failed to download {file_name}")

    # Zip the folder
    zip_path = f"/tmp/{folder_name}.zip"
    shutil.make_archive(zip_path.replace('.zip', ''), 'zip', download_dir)
    print(f"Zipped folder: {zip_path}")

    # Define S3 upload path
    s3_key = f"jobs_finished/{folder_name}.zip"

    # Upload zip to S3
    try:
        s3_client.upload_file(zip_path, bucket_name, s3_key)
        print(f"Uploaded {zip_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Failed to upload zip to S3", "error": str(e)})
        }

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": f"Downloaded {file_count} files, zipped, and uploaded to S3",
            "file_count": file_count,
            "s3_path": f"s3://{bucket_name}/{s3_key}"
        })
    }

# Load test event from your JSON file
if __name__ == "__main__":
    json_file_name = "48c0c82f-3d58-4f2f-b4de-6392a67a8272.json"
    bucket_name = "myrootfolder"

    with open(json_file_name) as f:
        event_data = json.load(f)
    
    response = lambda_handler(event_data, json_file_name, bucket_name)
    print(json.dumps(response, indent=2))


def list_files_in_s3(bucket_name, folder_name):
    """Fetches all files from the specified S3 bucket folder."""
    file_list = []
    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {"Bucket": bucket_name, "Prefix": folder_name}

    for page in paginator.paginate(**operation_parameters):
        if "Contents" in page:
            for obj in page["Contents"]:
                file_list.append(obj["Key"])  # Store full S3 path

    return file_list

def generate_presigned_urls(bucket_name, file_keys, expiration=3600):
    """Generates presigned URLs for each file."""
    presigned_urls = []
    for file_key in file_keys:
        try:
            url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket_name, "Key": file_key},
                ExpiresIn=expiration
            )
            presigned_urls.append({"file_name": os.path.basename(file_key), "url": url})
        except Exception as e:
            print(f"Error generating URL for {file_key}: {e}")
    
    return presigned_urls

def upload_json_to_s3(bucket_name, file_name, json_data):
    """Uploads JSON data to S3 inside 'presigned_urls/' folder."""
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(json_data, indent=4),
            ContentType="application/json"
        )
        print(f"Successfully uploaded {file_name} to {bucket_name}")
        return True
    except Exception as e:
        print(f"Error uploading JSON to S3: {e}")
        return False

def push_to_sqs(queue_url, message_body):
    """Pushes a single message containing all presigned URLs to SQS."""
    try:
        sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body)
        )
        print(f"Successfully sent message to SQS: {queue_url}")
    except Exception as e:
        print(f"Error sending message to SQS: {e}")

def invoke_external_lambda(endpoint, payload):
    """Invokes the external Lambda function using HTTP (POST)."""
    try:
        response = requests.post(endpoint, json=payload)
        if response.status_code == 200:
            print(f"Successfully invoked external Lambda endpoint: {endpoint}")
        else:
            print(f"Failed to invoke external Lambda endpoint. Status code: {response.status_code}")
        return response
    except Exception as e:
        print(f"Error invoking external Lambda endpoint: {e}")
        return None

def lambda1_prepare_job(event, context):
    """AWS Lambda entry point function."""
    try:
        # Step 1: Generate a unique job ID (using UUID)
        job_id = str(uuid.uuid4())  # Generate unique job ID

        # Step 2: Get all files from S3 folder
        file_keys = list_files_in_s3(S3_BUCKET_NAME, S3_FOLDER_NAME)
        
        if not file_keys:
            return {"statusCode": 404, "body": json.dumps({"message": "No files found in the folder"})}

        # Step 3: Generate presigned URLs
        presigned_urls = generate_presigned_urls(S3_BUCKET_NAME, file_keys)

        # Step 4: Create JSON structure with the target Lambda URL (External URL) and SQS
        output_data = {
            "job_id": job_id,
            "timestamp": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "files": [os.path.basename(f) for f in file_keys],
            "presigned_urls": presigned_urls,
            "target_lambda_url": LAMBDA_FUNCTION_URL,  # Use external Lambda function URL
            "external_lambda_endpoint": EXTERNAL_LAMBDA_ENDPOINT  # External Lambda endpoint for HTTP call
        }

        # Step 5: Create dynamic S3 file name using job_id
        output_json_file_name = OUTPUT_JSON_FILE_PATH.format(job_id=job_id)
        print(OUTPUT_JSON_FILE_PATH.format,"OUTPUT_JSON_FILE_PATH-----")
        print(output_json_file_name,"output_json_file_name--------")
        # Step 6: Upload JSON file to S3 inside "presigned_urls/" folder with job ID as file name
        uploaded = upload_json_to_s3(S3_BUCKET_NAME, output_json_file_name, output_data)

        # Step 7: Push presigned URLs to SQS
        if uploaded:
            push_to_sqs(SQS_QUEUE_URL, presigned_urls)

            # Step 8: Optionally invoke external Lambda function via HTTP endpoint
            invoke_external_lambda(EXTERNAL_LAMBDA_ENDPOINT, output_data)

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "JSON file uploaded successfully, presigned URLs pushed to SQS, and external Lambda invoked",
                    "output_file": output_json_file_name
                })
            }
        else:
            return {"statusCode": 500, "body": json.dumps({"message": "Failed to upload JSON to S3"})}

    except Exception as e:
        print(f"Error in Lambda function: {e}")
        return {"statusCode": 500, "body": json.dumps({"message": "Internal Server Error", "error": str(e)})}

