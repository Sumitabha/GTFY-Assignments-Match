# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(getAssignmentDetails) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import pyodbc
import os
import logging
import json
from azure.identity import DefaultAzureCredential

getAssignmentDetails = func.Blueprint()


@getAssignmentDetails.route(route="getAssignmentDetails, auth_level=func.AuthLevel.FUNCTION")
def getAssignmentDetailsById(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing request to fetch assignment by job ID')

    try:
        # Get job_id from query string
        job_id = req.params.get('job_id')
        if not job_id:
            return func.HttpResponse("Missing job_id parameter", status_code=400)

        # Environment vars
        server = os.getenv('DB_SERVER')
        database = os.getenv('DB_NAME')
        driver = '{ODBC Driver 18 for SQL Server}'

        # Get access token for Azure SQL
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")
        access_token = token.token.encode('utf-16-le')

        # Connect
        conn_str = (
            f'DRIVER={driver};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'Encrypt=yes;'
            f'TrustServerCertificate=yes;'
            f'Authentication=ActiveDirectoryMSI'
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Parameterized query
        cursor.execute("SELECT * FROM dbo.assignmentList WHERE id = ?", job_id)
        row = cursor.fetchone()
        conn.close()

        if not row:
            return func.HttpResponse(f"No assignment found with id {job_id}", status_code=404)

        columns = [column[0] for column in cursor.description]
        result = dict(zip(columns, row))

        return func.HttpResponse(json.dumps(result, default=str), status_code=200, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)