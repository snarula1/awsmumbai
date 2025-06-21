import json
import os
import urllib.request
import urllib.error
import logging
import sys
import zipfile
import shutil
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs
from datetime import datetime

# Try to import psutil, but don't fail if it's not available
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    logging.warning("psutil module not found. System statistics will not be available.")

# Configuration
SIMULTANEOUS_DOWNLOADS_MAX =  4  # Number of files to download simultaneously

# Constants
API_URL = "https://yknlfsjyye.execute-api.us-east-1.amazonaws.com/dev/prepare-job"

def setup_logging(job_id, local_folder_path):
    """Set up logging to both console and file"""
    # Create a log file in the timestamped job folder
    log_file = os.path.join(local_folder_path, f"{job_id}_log_file.txt")
    
    # Configure logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # Create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized for job {job_id}")
    logging.info(f"Log file: {log_file}")
    
    return log_file

def step_1_fetch_job_details():
    """Step 1: Fetch job details from the API"""
    logging.info(f"Fetching job details from {API_URL}")
    try:
        with urllib.request.urlopen(API_URL) as response:
            raw_response = response.read().decode('utf-8')
            logging.info(f"Raw API response: {raw_response[:200]}...")  # Log first 200 chars
            
            try:
                response_data = json.loads(raw_response)
                logging.info(f"Response data keys: {list(response_data.keys())}")
                
                # The body is a JSON string that needs to be parsed
                if 'body' not in response_data:
                    logging.error(f"Missing 'body' in response. Response keys: {list(response_data.keys())}")
                    raise Exception(f"API response missing 'body' field. Got keys: {list(response_data.keys())}")
                
                if isinstance(response_data['body'], str):
                    try:
                        body = json.loads(response_data['body'])
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to parse 'body' as JSON: {e}")
                        logging.error(f"Body content: {response_data['body'][:200]}...")
                        raise Exception(f"Failed to parse 'body' as JSON: {e}")
                else:
                    body = response_data['body']
                
                if not body:
                    logging.error("Body is empty or null")
                    raise Exception("API returned empty body")
                
                logging.info(f"Body keys: {list(body.keys()) if isinstance(body, dict) else 'Not a dictionary'}")
                
                job_id = body.get('job_id')
                if not job_id:
                    logging.error("Missing job_id in body")
                    raise Exception("API response missing job_id")
                    
                logging.info(f"Job ID: {job_id}")
                return body
                
            except json.JSONDecodeError as e:
                logging.error(f"Failed to parse API response as JSON: {e}")
                logging.error(f"Raw response: {raw_response[:500]}...")
                raise Exception(f"Failed to parse API response as JSON: {e}")
                
    except urllib.error.HTTPError as e:
        logging.error(f"Failed to fetch job details: {e.code} - {e.reason}")
        raise Exception(f"Failed to fetch job details: {e.code} - {e.reason}")
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {e.reason}")
        raise Exception(f"URL Error: {e.reason}")

def step_2_fetch_job_file(url):
    """Step 2: Fetch the job file using the presigned URL"""
    logging.info(f"Fetching job file from presigned URL")
    try:
        # Some presigned URLs may contain special characters that need to be handled
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            content = response.read().decode('utf-8')
            try:
                return json.loads(content)
            except json.JSONDecodeError as e:
                logging.error(f"Error parsing JSON: {e}")
                logging.error(f"Response content: {content[:200]}...")  # Print first 200 chars
                raise Exception(f"Invalid JSON response from presigned URL")
    except urllib.error.HTTPError as e:
        logging.error(f"Failed to fetch job file: {e.code} - {e.reason}")
        raise Exception(f"Failed to fetch job file: {e.code} - {e.reason}")
    except urllib.error.URLError as e:
        logging.error(f"URL Error: {e.reason}")
        raise Exception(f"URL Error: {e.reason}")

def step_4_create_files_subfolder(local_folder_path, job_id):
    """Step 4: Create a subfolder for the job files"""
    # Create subfolder with just the job_id
    files_folder_path = os.path.join(local_folder_path, job_id)
    os.makedirs(files_folder_path, exist_ok=True)
    logging.info(f"Created files subfolder: {files_folder_path}")
    
    return files_folder_path

def download_single_file(file_obj, files_folder_path, index, total_files, max_retries=1):
    """Download a single file using the presigned URL with retry logic"""
    # Import system_stats here to avoid circular imports
    from system_stats import format_system_stats
    
    file_name = file_obj.get("file_name")
    presigned_url = file_obj.get("presigned_url")
    
    if not file_name or not presigned_url:
        logging.warning(f"Missing file_name or presigned_url for file object {index+1}")
        return None
    
    # Log the presigned URL (first 100 chars for security)
    url_preview = presigned_url[:100] + "..." if len(presigned_url) > 100 else presigned_url
    logging.info(f"Presigned URL for {file_name}: {url_preview}")
    
    local_file_path = os.path.join(files_folder_path, file_name)
    
    # Try to download with retries
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                logging.info(f"Retry attempt {attempt} for file {file_name}")
            
            # Get current thread name for tracking
            thread_name = threading.current_thread().name
            
            # Get system stats
            system_stats_str = format_system_stats()
            
            # Log download start with system stats
            logging.info(f"Downloading file {index+1}/{total_files}: {file_name} [Thread: {thread_name}] {system_stats_str}")
            
            # Create request with headers
            req = urllib.request.Request(presigned_url, headers={'User-Agent': 'Mozilla/5.0'})
            
            # Download and save the file
            with urllib.request.urlopen(req) as response, open(local_file_path, 'wb') as out_file:
                out_file.write(response.read())
            
            # Get system stats after download
            system_stats_str = format_system_stats()
            logging.info(f"Successfully saved file to: {local_file_path} {system_stats_str}")
                
            return local_file_path
            
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error downloading file {file_name} (attempt {attempt+1}/{max_retries+1}): {error_msg}")
            
            # If this was the last retry, give up
            if attempt == max_retries:
                logging.error(f"All retry attempts failed for {file_name}")
                return None
            
            # Wait a bit before retrying (exponential backoff)
            retry_wait = 2 ** attempt  # 1, 2, 4, 8, etc. seconds
            logging.info(f"Waiting {retry_wait} seconds before retry...")
            import time
            time.sleep(retry_wait)

def step_5_download_files(job_data, files_folder_path, max_retries=1):
    """Step 5: Download all files from the presigned URLs using parallel downloads"""
    # Record start time for downloads
    download_start_time = datetime.now()
    # Extract file objects from job data
    try:
        file_objects = job_data.get("JobRequest", {}).get("file_object", [])
    except AttributeError:
        logging.error("Error: Job data is not in expected format")
        return []
    
    if not file_objects:
        logging.warning("No files to download")
        return []
    
    total_files = len(file_objects)
    
    # Adjust the number of workers to not exceed the number of files
    actual_workers = min(SIMULTANEOUS_DOWNLOADS_MAX, total_files)
    
    logging.info(f"Starting parallel download of {total_files} files with {actual_workers} simultaneous downloads")
    logging.info(f"Retry attempts configured: {max_retries}")
    
    downloaded_files = []
    active_downloads = 0
    completed_downloads = 0
    download_status_lock = threading.Lock()
    
    # Use ThreadPoolExecutor to download files in parallel
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        # Submit all download tasks
        future_to_file = {
            executor.submit(
                download_single_file, 
                file_obj, 
                files_folder_path, 
                i, 
                total_files,
                max_retries
            ): (i, file_obj.get("file_name", f"file_{i}")) 
            for i, file_obj in enumerate(file_objects)
        }
        
        # Track active downloads
        with download_status_lock:
            active_downloads = len(future_to_file)
            logging.info(f"Files processed already - done: {completed_downloads}, files currently processing active downloads: {active_downloads}")
        
        # Process results as they complete
        for future in future_to_file:
            file_index, file_name = future_to_file[future]
            try:
                local_file_path = future.result()
                if local_file_path:
                    downloaded_files.append(local_file_path)
                
                # Update download status
                with download_status_lock:
                    completed_downloads += 1
                    active_downloads = len(future_to_file) - completed_downloads
                    logging.info(f"Files processed already - done: {completed_downloads}, files currently processing active downloads: {active_downloads}")
                    
            except Exception as e:
                logging.error(f"Exception while downloading file {file_name}: {str(e)}")
                
                # Update download status even on error
                with download_status_lock:
                    completed_downloads += 1
                    active_downloads = len(future_to_file) - completed_downloads
                    logging.info(f"Files processed already - done: {completed_downloads}, files currently processing active downloads: {active_downloads}")
    
    # Calculate download time
    download_end_time = datetime.now()
    download_time = download_end_time - download_start_time
    download_seconds = download_time.total_seconds()
    
    logging.info(f"Downloaded {len(downloaded_files)} files out of {total_files}")
    logging.info(f"Total download time: {download_seconds:.2f} seconds ({download_time})")
    
    return downloaded_files

def step_6_zip_folder(local_folder_path, job_id):
    """Step 6: Zip the entire folder"""
    # Create zip file path in the timestamped job folder
    zip_file_path = os.path.join(local_folder_path, f"{job_id}_archive.zip")
    
    logging.info(f"Creating zip archive: {zip_file_path}")
    
    # Track file sizes for analysis
    total_size_before = 0
    files_processed = 0
    
    try:
        # Method 1: Using zipfile module (works on both Windows and Linux)
        with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Walk through all files in the folder
            for root, dirs, files in os.walk(local_folder_path):
                for file in files:
                    # Calculate path for file in zip
                    file_path = os.path.join(root, file)
                    
                    # Skip the zip file itself if it somehow exists already
                    if file_path == zip_file_path:
                        continue
                    
                    # Get file size
                    file_size = os.path.getsize(file_path)
                    total_size_before += file_size
                    files_processed += 1
                    
                    # Calculate arcname (path relative to the folder being zipped)
                    arcname = os.path.relpath(file_path, os.path.dirname(local_folder_path))
                    
                    # Add file to zip
                    zipf.write(file_path, arcname)
                    logging.info(f"Added to zip: {file_path} ({file_size} bytes)")
        
        # Get the size of the zip file
        zip_size = os.path.getsize(zip_file_path)
        
        # Calculate compression stats
        compression_savings = total_size_before - zip_size
        compression_ratio = (compression_savings / total_size_before) * 100 if total_size_before > 0 else 0
        
        # Log the results
        logging.info(f"File size analysis:")
        logging.info(f"  - Total files processed: {files_processed}")
        logging.info(f"  - Total size before compression: {total_size_before} bytes ({total_size_before/1024:.2f} KB)")
        logging.info(f"  - Size after compression: {zip_size} bytes ({zip_size/1024:.2f} KB)")
        logging.info(f"  - Space saved: {compression_savings} bytes ({compression_savings/1024:.2f} KB)")
        logging.info(f"  - Compression ratio: {compression_ratio:.2f}%")
        
        logging.info(f"Successfully created zip archive: {zip_file_path}")
        return zip_file_path, total_size_before, zip_size, files_processed
    
    except Exception as e:
        logging.error(f"Error creating zip archive: {str(e)}")
        
        # Fallback method for Linux systems
        try:
            logging.info("Trying fallback zip method using shutil...")
            # Get the files subfolder path
            files_subfolder = os.path.join(local_folder_path, job_id)
            
            # Use shutil to create zip file
            zip_base_path = os.path.join(local_folder_path, job_id)
            shutil.make_archive(zip_base_path, 'zip', files_subfolder)
            
            # Get the size of the zip file
            zip_size = os.path.getsize(zip_file_path)
            
            # For fallback method, we don't have detailed size info, so estimate
            total_size_before = zip_size  # Conservative estimate
            files_processed = 1  # At least one file
            
            logging.info(f"Successfully created zip archive using fallback method: {zip_file_path}")
            return zip_file_path, total_size_before, zip_size, files_processed
        except Exception as e2:
            logging.error(f"Error in fallback zip method: {str(e2)}")
            return None, 0, 0, 0

def step_7_get_upload_url_and_upload(callback_url, zip_file_path):
    """Step 7: Get upload URL and upload the zip file"""
    logging.info(f"Getting upload URL from: {callback_url}")
    
    try:
        # Step 7.1: Get the upload URL
        req = urllib.request.Request(callback_url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req) as response:
            content = response.read().decode('utf-8')
            response_data = json.loads(content)
            
            # Check if response is wrapped in a body field
            if isinstance(response_data, dict) and 'body' in response_data and isinstance(response_data['body'], str):
                response_data = json.loads(response_data['body'])
            
            logging.info(f"Upload URL response: {json.dumps(response_data)}")
            
            # Extract the upload URL
            upload_url = response_data.get('upload_url')
            s3_key = response_data.get('s3_key')
            
            if not upload_url:
                raise Exception("No upload_url found in response")
            
            logging.info(f"Got upload URL for S3 key: {s3_key}")
            
            # Step 7.2: Upload the zip file
            logging.info(f"Uploading zip file to: {upload_url}")
            
            # Read the zip file
            with open(zip_file_path, 'rb') as file:
                zip_data = file.read()
            
            # Create a PUT request with the zip data
            upload_req = urllib.request.Request(
                url=upload_url,
                data=zip_data,
                method='PUT',
                headers={
                    'Content-Type': 'application/zip',
                    'User-Agent': 'Mozilla/5.0'
                }
            )
            
            # Send the request
            with urllib.request.urlopen(upload_req) as upload_response:
                status = upload_response.status
                logging.info(f"Upload completed with status: {status}")
                
                if status >= 200 and status < 300:
                    logging.info(f"Successfully uploaded zip file to S3: {s3_key}")
                    return True, s3_key
                else:
                    logging.error(f"Upload failed with status: {status}")
                    return False, None
                
    except Exception as e:
        logging.error(f"Error in step 7: {str(e)}")
        return False, None

def main_process_job():
    """Main function to orchestrate the job processing steps"""
    # Record start time
    start_time = datetime.now()
    
    try:
        # Step 1: Fetch job details from API
        job_details = step_1_fetch_job_details()
        job_id = job_details.get('job_id')
        job_file_download_url = job_details.get('job_file_download_url')
        
        if not job_id or not job_file_download_url:
            raise Exception("Missing job_id or job_file_download_url in response")
        
        # Create timestamp folder first (we need this for logging)
        # Get current timestamp in yymmddhhmmss format
        timestamp = datetime.now().strftime("%y%m%d%H%M%S")
        
        # Create folder name with timestamp and job_id
        folder_name = f"{timestamp}-{job_id}"
        
        # Create local folder
        local_folder_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "process_work", folder_name)
        os.makedirs(local_folder_path, exist_ok=True)
        print(f"Created local folder: {local_folder_path}")
        
        # Set up logging in the job folder
        log_file = setup_logging(job_id, local_folder_path)
        
        logging.info(f"Using download URL: {job_file_download_url}")
        
        # Step 2: Fetch the job file using the presigned URL
        job_file_data = step_2_fetch_job_file(job_file_download_url)
        
        # Validate job file data
        if not job_file_data:
            raise Exception("Empty job file data received")
            
        # Log job file structure
        print(f"Job file structure: {list(job_file_data.keys()) if isinstance(job_file_data, dict) else 'Not a dictionary'}")
        
        # Step 3: Save the job file locally
        local_file_path = os.path.join(local_folder_path, "job_details.json")
        with open(local_file_path, 'w') as f:
            json.dump(job_file_data, f, indent=2)
        logging.info(f"Saved job details to local file: {local_file_path}")
        
        # Step 4: Create a subfolder for the job files
        files_folder_path = step_4_create_files_subfolder(local_folder_path, job_id)
        
        # Step 5: Download all files from the presigned URLs
        downloaded_files = step_5_download_files(job_file_data, files_folder_path)
        
        # Step 6: Zip the entire folder
        zip_result = step_6_zip_folder(local_folder_path, job_id)
        
        if not zip_result or not zip_result[0]:
            raise Exception("Failed to create zip archive")
            
        zip_file_path, total_size_before, zip_size, files_processed = zip_result
        
        # Get the callback URL from the job data
        callback_url = job_file_data.get("JobRequest", {}).get("call_url_when_done_with_job_id")
        
        if not callback_url:
            logging.error("No callback URL found in job data")
            raise Exception("No callback URL found in job data")
        
        logging.info(f"Callback URL: {callback_url}")
        
        # Step 7: Get upload URL and upload the zip file
        upload_success, s3_key = step_7_get_upload_url_and_upload(callback_url, zip_file_path)
        
        if not upload_success:
            raise Exception("Failed to upload zip file")
        
        # Calculate elapsed time
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        elapsed_seconds = elapsed_time.total_seconds()
        
        # Calculate total file size in MB
        total_size_mb = total_size_before / (1024 * 1024)
        zip_size_mb = os.path.getsize(zip_file_path) / (1024 * 1024)
        zip_filename = os.path.basename(zip_file_path)
        
        logging.info(f"Successfully processed job {job_id}")
        logging.info(f"Job file saved locally at: {local_file_path}")
        logging.info(f"Downloaded {len(downloaded_files)} files to: {files_folder_path}")
        logging.info(f"Created zip archive at: {zip_file_path}")
        logging.info(f"Uploaded zip file to S3: {s3_key}")
        logging.info(f"Total processing time: {elapsed_seconds:.2f} seconds ({elapsed_time}) to download {len(downloaded_files)} files amounting to {total_size_mb:.2f} MB in size and then zipping and uploading {zip_size_mb:.2f} MB of {zip_filename}.")
        
        return {
            "job_id": job_id,
            "local_folder_path": local_folder_path,
            "files_folder_path": files_folder_path,
            "downloaded_files": downloaded_files,
            "zip_file_path": zip_file_path,
            "s3_key": s3_key,
            "log_file": log_file,
            "processing_time_seconds": elapsed_seconds
        }
        
    except Exception as e:
        # Calculate elapsed time even in case of error
        end_time = datetime.now()
        elapsed_time = end_time - start_time
        elapsed_seconds = elapsed_time.total_seconds()
        
        if 'logging' in sys.modules and logging.getLogger().handlers:
            logging.error(f"Error processing job: {str(e)}")
            logging.info(f"Failed processing time: {elapsed_seconds:.2f} seconds ({elapsed_time})")
        else:
            print(f"Error processing job: {str(e)}")
            print(f"Failed processing time: {elapsed_seconds:.2f} seconds")
        
        # Don't re-raise to prevent script from crashing
        return {
            "error": str(e),
            "processing_time_seconds": elapsed_seconds
        }

if __name__ == "__main__":
    try:
        result = main_process_job()
        if "error" in result:
            logging.error(f"Process completed with error: {result['error']}")
        else:
            logging.info(f"Process completed successfully for job: {result['job_id']}")
    except KeyboardInterrupt:
        logging.warning("\nProcess interrupted by user")
    except Exception as e:
        print(f"Unhandled exception: {str(e)}")
    finally:
        logging.info("Process finished")