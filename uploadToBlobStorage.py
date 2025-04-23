import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import os

uploadToBlobStorage = func.Blueprint()

# Set via environment variable or directly here
BLOB_CONNECTION_STRING = os.getenv("AZURE_BLOB_CONN")  # Replace if needed
BLOB_CONTAINER_NAME = "gtfydemo"
BLOB_FOLDER_PATH = "resume"  # e.g., "uploads", "resumes", etc.

@uploadToBlobStorage.route(route="uploadToBlobStorage", methods=["POST"])
def uploadFilesToBlobStorage(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for file upload.')

    try:
        # Get the file from the HTTP request
        file = req.files.get('file')
        if not file:
            return func.HttpResponse("No file uploaded in the 'file' field.", status_code=400)

        file_name = file.filename
        file_content = file.stream.read()

        # Construct the blob path using folder + file name
        blob_path = f"{BLOB_FOLDER_PATH}/{file_name}"

        # Connect to Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_path)

        # Upload file
        blob_client.upload_blob(file_content, overwrite=True)

        return func.HttpResponse(
            f"File '{file_name}' uploaded successfully to folder '{BLOB_FOLDER_PATH}' in Blob Storage.",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        return func.HttpResponse(f"Error uploading file: {str(e)}", status_code=500)
