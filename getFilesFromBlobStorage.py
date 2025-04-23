import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import os
import json
from datetime import datetime, timedelta
import ntpath

getFilesFromBlobStorage = func.Blueprint()

# Environment or hardcoded configuration
BLOB_CONNECTION_STRING = os.getenv("AZURE_BLOB_CONN")
BLOB_CONTAINER_NAME = "gtfydemo"
BLOB_FOLDER_PATH = "resume"  # e.g., "uploads", "resumes", etc.

# SAS link expiry in minutes
SAS_EXPIRY_MINUTES = 60

@getFilesFromBlobStorage.route(route="getFilesFromBlobStorage", methods=["GET"])
def getResumesFromBlobStorage(req: func.HttpRequest) -> func.HttpResponse:
    logging.info(f'Listing files from folder: {BLOB_FOLDER_PATH}')

    try:
        prefix = f"{BLOB_FOLDER_PATH}/"
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

        files = []
        for blob in container_client.list_blobs(name_starts_with=prefix):
            filename = ntpath.basename(blob.name)

            sas_token = generate_blob_sas(
                account_name=blob_service_client.account_name,
                container_name=BLOB_CONTAINER_NAME,
                blob_name=blob.name,
                account_key=blob_service_client.credential.account_key,
                permission=BlobSasPermissions(read=True),
                expiry=datetime.utcnow() + timedelta(minutes=SAS_EXPIRY_MINUTES)
            )
            download_url = f"https://{blob_service_client.account_name}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob.name}?{sas_token}"

            files.append({
                "name": filename,
                "size": blob.size,
                "last_modified": blob.last_modified.strftime("%Y-%m-%d %H:%M:%S"),
                "download_url": download_url
            })

        return func.HttpResponse(
            json.dumps({"files": files}, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.error(f"Error listing files: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
