import logging
from openai import AzureOpenAI
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from openai import OpenAI
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
import tempfile
import json
from getAssignmentDetails import getAssignmentDetails
from uploadToBlobStorage import uploadToBlobStorage
from getFilesFromBlobStorage import getFilesFromBlobStorage
from enhanceCV import enhanceCV

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_functions(getAssignmentDetails)
app.register_functions(uploadToBlobStorage)
app.register_functions(getFilesFromBlobStorage)
app.register_functions(enhanceCV)

def extract_resume_text(filepath):
    ext = os.path.splitext(filepath)[-1].lower()

    if ext == ".pdf":
        import fitz
        with fitz.open(filepath) as doc:
            return "\n".join(page.get_text() for page in doc)

    elif ext == ".docx":
        from docx import Document
        doc = Document(filepath)
        return "\n".join(p.text for p in doc.paragraphs)

    elif ext == ".txt":
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()

    else:
        raise ValueError("Unsupported file type: " + ext)

def get_latest_resume_from_folder(blob_service_client, container_name, folder_prefix):
    container_client = blob_service_client.get_container_client(container_name)
    blobs = list(container_client.list_blobs(name_starts_with=folder_prefix))

    if not blobs:
        raise FileNotFoundError(f"No files found in folder: {folder_prefix}")

    latest_blob = max(blobs, key=lambda b: b.last_modified)
    return latest_blob.name 

@app.route(route="assignmentsMatch")
def assignmentsMatch(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing resume for job matching from predefined blob folder")

    try:
        container_name = "gtfydemo"
        folder_prefix = "resume/"

        blob_service = BlobServiceClient.from_connection_string(os.environ["AZURE_BLOB_CONN"])
        blob_name = get_latest_resume_from_folder(blob_service, container_name, folder_prefix)
        blob_client = blob_service.get_blob_client(container=container_name, blob=blob_name)

        with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(blob_name)[-1]) as tmp_file:
            tmp_file.write(blob_client.download_blob().readall())
            tmp_path = tmp_file.name

        resume_text = extract_resume_text(tmp_path)
        os.remove(tmp_path)

        # Set up Azure OpenAI config
        client = AzureOpenAI(
            api_key=os.environ["AZURE_OPENAI_KEY"],
            api_version=os.environ["AZURE_OPENAI_API_VERSION"],
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"]
        )

        response = client.chat.completions.create(
            model=os.environ["AZURE_OPENAI_DEPLOYMENT"],
            messages=[
                {"role": "system", "content": "You are an AI assistant that extracts job-related search queries from resumes."},
                {"role": "user", "content": f"Here is a resume:\n\n{resume_text}\n\nPlease extract relevant job title and skills as a query."}
            ]
        )

        search_query = response.choices[0].message.content.strip()

        search_client = SearchClient(
            endpoint=os.environ["SEARCH_ENDPOINT"],
            index_name=os.environ["SEARCH_INDEX"],
            credential=AzureKeyCredential(os.environ["SEARCH_KEY"])
        )

        results = list(search_client.search(
            search_query,
            top=20,
            highlight_fields="job_desc,req_skills",  # ✅ Enable highlights here
            select="id,title,company,location,type"
        ))

        logging.info(f"Search returned {len(results)} results.")

        max_score = results[0]['@search.score'] if results else 1.0

        jobs = []
        for doc in results:
            raw_score = doc['@search.score']
            match_percent = int((raw_score / max_score) * 100)

            jobs.append({
                "id": doc.get("id", ""),
                "title": doc.get("title", ""),
                "company": doc.get("company", ""),
                "location": doc.get("location", ""),
                "type": doc.get("type", ""),
                "matchPercent": match_percent,
                "logoUrl": doc.get("logoUrl", None),
                "highlights": doc.get("@search.highlights", {})  # ✅ Include highlights
            })

        return func.HttpResponse(
            json.dumps(jobs, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("Error occurred while processing resume.")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)