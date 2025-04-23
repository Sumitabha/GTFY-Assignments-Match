import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import os

uploadToBlobStorage = func.Blueprint()

# Set via environment variable or directly here
BLOB_CONNECTION_STRING = os.getenv("AZURE_BLOB_CONN")
BLOB_CONTAINER_NAME = "gtfydemo"
BLOB_FOLDER_PATH = "resume"  # Folder inside the container

@uploadToBlobStorage.route(route="uploadToBlobStorage", methods=["POST"])
def uploadFilesToBlobStorage(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for file upload.')

    try:
        file = req.files.get('file')
        if not file:
            return func.HttpResponse("No file uploaded in the 'file' field.", status_code=400)

        file_name = file.filename
        file_content = file.stream.read()
        blob_path = f"{BLOB_FOLDER_PATH}/{file_name}"

        # Connect to Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

        # Delete existing files in the folder
        prefix = f"{BLOB_FOLDER_PATH}/"
        for blob in container_client.list_blobs(name_starts_with=prefix):
            container_client.delete_blob(blob.name)
            logging.info(f"Deleted existing blob: {blob.name}")

        # Upload new file
        blob_client = container_client.get_blob_client(blob=blob_path)
        blob_client.upload_blob(file_content, overwrite=True)

        return func.HttpResponse(
            f"File '{file_name}' uploaded successfully to folder '{BLOB_FOLDER_PATH}' in Blob Storage (after clearing old files).",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error uploading file: {str(e)}")
        return func.HttpResponse(f"Error uploading file: {str(e)}", status_code=500)
