import logging
import os
import tempfile
import json
from collections import defaultdict
from datetime import datetime, timedelta

import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
from azure.search.documents import SearchClient
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.models import VectorizedQuery
from openai import AzureOpenAI
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.core.credentials import AzureKeyCredential as DocumentKeyCredential

from getAssignmentDetails import getAssignmentDetails
from uploadToBlobStorage import uploadToBlobStorage
from getFilesFromBlobStorage import getFilesFromBlobStorage
from enhanceCV import enhanceCV

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

app.register_functions(getAssignmentDetails)
app.register_functions(uploadToBlobStorage)
app.register_functions(getFilesFromBlobStorage)
app.register_functions(enhanceCV)

def extract_text_from_docx_with_layout_model(blob_url: str) -> str:
    endpoint = os.environ["AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT"]
    key = os.environ["AZURE_DOCUMENT_INTELLIGENCE_KEY"]

    client = DocumentIntelligenceClient(
        endpoint=endpoint,
        credential=DocumentKeyCredential(key)
    )

    poller = client.begin_analyze_document(
        model_id="prebuilt-layout",
        body=AnalyzeDocumentRequest(url_source=blob_url),
        content_type="application/json"
    )

    result = poller.result()
    lines = []

    for page in result.pages:
        if page.lines:
            for line in page.lines:
                text = line.content.strip()
                if text:
                    lines.append(text)

    if result.tables:
        for table in result.tables:
            for cell in table.cells:
                text = cell.content.strip()
                if text:
                    lines.append(text)

    full_text = "\n".join(lines)
    return full_text

def get_latest_resume_from_folder(blob_service_client, container_name, folder_prefix):
    container_client = blob_service_client.get_container_client(container_name)
    blobs = list(container_client.list_blobs(name_starts_with=folder_prefix))

    if not blobs:
        raise FileNotFoundError(f"No files found in folder: {folder_prefix}")

    latest_blob = max(blobs, key=lambda b: b.last_modified)
    return latest_blob.name

def generate_blob_sas_url(blob_service_client, container, blob_name):
    blob_client = blob_service_client.get_blob_client(container=container, blob=blob_name)

    sas_token = generate_blob_sas(
        account_name=blob_service_client.account_name,
        container_name=container,
        blob_name=blob_name,
        account_key=os.environ["AZURE_BLOB_KEY"],
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(minutes=15)
    )

    return f"{blob_client.url}?{sas_token}"

def parse_resume_with_gpt(resume_text: str) -> dict:
    client = AzureOpenAI(
        api_key=os.environ["AZURE_OPENAI_KEY"],
        api_version=os.environ["AZURE_OPENAI_API_VERSION"],
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"]
    )

    system_prompt = "You are an expert resume parser. Convert plain resume text into structured JSON."
    user_prompt = f"""
        Given the following resume text, extract structured information with this format:

        {{
        "full_name": "",
        "location": "",
        "summary": "",
        "skills": [],
        "certifications": [],
        "work_experience": [
            {{
            "company": "",
            "title": "",
            "start_date": "",
            "end_date": "",
            "responsibilities": ""
            }}
        ],
        "education": "",
        "languages": [],
        "publications": []
        }}

        Resume:
        {resume_text}
        """

    response = client.chat.completions.create(
        model=os.environ["AZURE_OPENAI_DEPLOYMENT"],
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0.3
    )

    output = response.choices[0].message.content.strip()

    try:
        return json.loads(output)
    except json.JSONDecodeError:
        logging.warning("GPT returned unparseable JSON. Returning raw text.")
        return {"raw_output": output}

@app.route(route="assignmentsMatch")
def assignmentsMatch(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing resume for hybrid matching...")

    try:
        container_name = "gtfydemo"
        folder_prefix = "resume/"

        blob_service = BlobServiceClient.from_connection_string(os.environ["AZURE_BLOB_CONN"])
        blob_name = get_latest_resume_from_folder(blob_service, container_name, folder_prefix)
        blob_url = generate_blob_sas_url(blob_service, container_name, blob_name)

        resume_text = extract_text_from_docx_with_layout_model(blob_url)
        structured_resume = parse_resume_with_gpt(resume_text)

        if "raw_output" in structured_resume:
            raw_output = structured_resume["raw_output"]

            if raw_output.strip().startswith("```"):
                raw_output = raw_output.strip().strip("`").strip("json").strip()

            try:
                structured_resume = json.loads(raw_output)
            except json.JSONDecodeError:
                logging.warning("Failed to parse JSON from raw_output.")
                structured_resume = {}

        summary = structured_resume.get("summary", "")
        skills = structured_resume.get("skills", [])

        client = AzureOpenAI(
            api_key=os.environ["AZURE_OPENAI_KEY"],
            api_version=os.environ["AZURE_OPENAI_API_VERSION"],
            azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"]
        )

        # Extract keywords from summary and skills via GPT
        keyword_response = client.chat.completions.create(
            model=os.environ["AZURE_OPENAI_DEPLOYMENT"],
            messages=[
                {"role": "system", "content": "Extract 15-20 relevant keywords from the following text for search purposes. Return them as a comma-separated list."},
                {"role": "user", "content": f"Summary:\n{summary}\nSkills:\n{', '.join(skills)}"}
            ],
            temperature=0.3
        )

        search_keywords = keyword_response.choices[0].message.content.strip()
        logging.info(f"Search keywords extracted: {search_keywords}")

        embed_response = client.embeddings.create(
            input=[search_keywords],
            model=os.environ["AZURE_OPENAI_EMBEDDING_DEPLOYMENT"]
        )
        resume_vector = embed_response.data[0].embedding

        search_client = SearchClient(
            endpoint=os.environ["SEARCH_ENDPOINT"],
            index_name=os.environ["SEARCH_INDEX"],
            credential=AzureKeyCredential(os.environ["SEARCH_KEY"])
        )

        vector_query = VectorizedQuery(
            kind="vector",
            vector=resume_vector,
            fields="embedding",
            k_nearest_neighbors=50,
            profile="gtfy-vector-profile"
        )

        results = list(search_client.search(
            search_text=search_keywords,
            vector_queries=[vector_query],
            query_type="semantic",
            semantic_configuration_name="gtfy-semantic-config",
            query_caption="extractive",
            query_answer="extractive",
            highlight_fields="req_skills, key_responsibilities, job_desc",
            select=["id", "title", "company", "location", "type", "gtd_id", "req_skills", "key_responsibilities"]
        ))

        global_max_score = max((doc["@search.score"] for doc in results), default=1.0)
        job_map = defaultdict(list)
        for doc in results:
            job_id = doc.get("gtd_id") or doc["id"].split("_")[0]
            job_map[job_id].append(doc)

        final_jobs = []
        for job_id, chunks in job_map.items():
            chunks.sort(key=lambda d: d["@search.score"], reverse=True)
            top_doc = chunks[0]
            match_percent = int((top_doc["@search.score"] / global_max_score) * 100)

            highlightedSkills = []
            for doc in chunks:
                highlights = doc.get("@search.highlights")
                if highlights:
                    highlightedSkills.extend(highlights.get("req_skills", []))
                    highlightedSkills.extend(highlights.get("key_responsibilities", []))
                    highlightedSkills.extend(highlights.get("job_desc", []))
            highlightedSkills = list(set(highlightedSkills))

            final_jobs.append({
                "id": job_id,
                "title": top_doc.get("title", ""),
                "company": top_doc.get("company", ""),
                "location": top_doc.get("location", ""),
                "type": top_doc.get("type", ""),
                "req_skills": top_doc.get("req_skills"),
                "key_responsibilities": top_doc.get("key_responsibilities"),
                "matchPercent": match_percent,
                "highlightedSkills": highlightedSkills,
                "matchedChunkCount": len(chunks)
            })

        final_jobs.sort(key=lambda j: j["matchPercent"], reverse=True)

        return func.HttpResponse(
            json.dumps({
                "matched_jobs": final_jobs
            }, indent=2),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("Error occurred during job matching.")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
