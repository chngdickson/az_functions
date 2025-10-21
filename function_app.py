import azure.functions as func

from helper_funcs.container_job_manager import CreateContainerAppsManager2
from helper_funcs.blob_storage_manager import Blob_List_Manager
from helper_funcs.pubsub_manager import PubSubManager

import json
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Trigger Container Instances
@app.function_name(name="process_pointcloud")
@app.route(route="process_pointcloud")
def process_pointcloud(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
    logger.info(f"\n\n\nJFNJKWENFKJWFNWKJFNJKFNJKW\n\n\n")

    if name:
        return func.HttpResponse(f"Hello, {name},This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             f"This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )


# Retrieve Many Instance tokens
@app.function_name(name="retrieveInstanceMessageTokens")
@app.route(route="retrieveInstanceMessageTokens")
def retrieveInstanceMessageTokens(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('retrieveInstanceMessageTokens function processed a request.')
    blob_lst_mng = Blob_List_Manager()
    pubsub_mng = PubSubManager()
    blob_lst_mng.cleanConfig_via_expire_time()
    instance_lists = blob_lst_mng.get_data()
    
    if len(instance_lists) > 0:
        pubsub_read_tokens = pubsub_mng.get_write_tokens(instance_lists)
        result = {
            "status": "success",
            "group_names": pubsub_read_tokens,
            "message" : f"Here are your temporary Tokens, expiring in {pubsub_mng.minutes_to_expire} mins",
            "blob_token": blob_lst_mng.generate_sas_token()
        }
        status_code = 200
    else:
        pubsub_read_tokens = {}
        result = {
            "status": "unsuccessful",
            "group_names": pubsub_read_tokens,
            "message" : f"No Containers are running"
        }
        status_code = 503

    return func.HttpResponse(
        json.dumps(result),
        status_code=status_code,
        mimetype="application/json"
    )
 
@app.function_name(name="getBlobToken")
@app.route(route="getBlobToken")
def getBlobToken(req:func.HttpRequest) -> func.HttpResponse:
    blob_lst_mng = Blob_List_Manager()
    try:
        primary_endpoint, sas_token, container_name, blob_name = blob_lst_mng.generate_sas_token()
        status_code = 200
        result = {
            "status": "success!!",
            "primary_endpoint": primary_endpoint,
            "sas_token": sas_token,
            "container_name": container_name,
            "blob_name": blob_name,
            "message" : "Here is your token"
        }
    except Exception as e:
        status_code = 503
        result = {
            "status": "unsuccessful",
            "upload_url": "url",
            "sas_token": "",
            "message" : f"Encountered Error with Uploading {e}"
        }
    return func.HttpResponse(
        json.dumps(result),
        status_code=status_code,
        mimetype="application/json"
    )

# Process Uploaded message from Blob Container
@app.function_name(name="processUploadedFile")
@app.blob_trigger(
    arg_name="blob_req",
    direction="in",
    path="tph-files/pointcloudUploads/{name}.{blobextension}", 
    connection="ConnString_StoragePcd",
    fileName="@triggerBody().fileName"
    )
def processUploadedFile(blob_req: func.InputStream):
    """ 
    When Files are Uploaded and Completed in Blob, it will add this file in.
        "filename" : filename,
        "file_extension" : file_extention,
        "full_path" : full_path,
        "expire_time" : ""
    """
    logger.info(f"Python blob trigger function Begin blob. "
                f"Name: {blob_req.name}, "
                f"Blob Size: {blob_req.length} bytes")
    full_file_path:str = blob_req.name
    # blob_mng_obj = Blob_List_Manager()
    # blob_mng_obj.append(full_file_path)
    # pubsub_mng = PubSubManager()
    # instance_name, _ = blob_mng_obj._clean_the_string(full_file_path)
    # url_write_token = pubsub_mng._get_write_token(instance_name)
    # container_obj = CreateContainerAppsManager2()
    # container_obj.run_job(instance_name, url_write_token)