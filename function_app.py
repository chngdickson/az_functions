import os
import json
import logging
from pathlib import Path
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
import azure.functions as func
import azurefunctions.extensions.bindings.blob as blob
# app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

from helper_funcs.container_job_manager import CreateContainerAppsManager2
from helper_funcs.blob_data_tables_manager import TableEntityManager
from helper_funcs.pubsub_manager import PubSubManager
from helper_funcs.helper import clean_the_string, get_time_now, time_human_readable
ACA_JOBS = CreateContainerAppsManager2()
ACA_JOBS._init_job_params()

# Retrieve Single PubSub tokens of the Azure Container App Job Instance OMG
@app.function_name(name="getTokenPubSub")
@app.route(route="getTokenPubSub")
def getTokenPubSub(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        filename = req_body.get('filename')
        pubsub_mng = PubSubManager()
        
        instance_name, _ = clean_the_string(filename)
        pubsub_read_tokens = pubsub_mng._get_read_token(instance_name)
        result = {
            "status": "success",
            "group_name": instance_name,
            "url_token" : pubsub_read_tokens,
            "message" : f"Here are your temporary Tokens, expiring in {pubsub_mng.minutes_to_expire} mins",
        }
        status_code = 200
    except Exception as e:
        return func.HttpResponse(
            json.dumps({
                "status":"unsuccessful",
                "message":f"PubsubToken Error with error: [{e}]"
                }),
            status_code=503,
            mimetype="application/json"
        )

    return func.HttpResponse(
        json.dumps(result),
        status_code=status_code,
        mimetype="application/json"
    )

@app.function_name(name="getBlobToken")
@app.route(route="getBlobToken")
async def getBlobToken(req:func.HttpRequest) -> func.HttpResponse:
    try:
        logger.warning(f"getBlobToken started")
        req_body = req.get_json()
        full_file_path = req_body.get('filename')
        gigabytes = req_body.get('filesizeInGB')
        data_storage_manager = TableEntityManager()
        data_storage_manager.upload_file_infos(full_file_path)
        
        
        can_upload, db_init_dict = await data_storage_manager.can_upload(full_file_path, gigabytes)
        if can_upload:
            primary_endpoint, sas_token, container_name, blob_name = data_storage_manager.upload_file_infos(full_file_path)
            await data_storage_manager.delete_file_on_pcd_upload(primary_endpoint, sas_token, container_name, blob_name)
            # Start Container
            ACA_Manager = CreateContainerAppsManager2()
            if ACA_Manager.acaJobsStarted(PubSubManager(), data_storage_manager, full_file_path, db_init_dict):
                logger.warning("ACA JOBS STARTED SUCCESSFULLY")
                
                # Delete File on Upload
                logger.warning("Deleting Files On Upload")
                # await data_storage_manager.delete_process_folder_on_pcd_upload(data_storage_manager.folder_container, db_init_dict["process_folder"])
                # logger.warning("Deleting Files Completed")
                
                status_code = 200
                result = {
                    "status": "success",
                    "primary_endpoint": primary_endpoint,
                    "sas_token": sas_token,
                    "container_name": container_name,
                    "blob_name": blob_name,
                    "message" : "Here is your token"
                }
            else:
                logger.error("ACA JOBS Did not Start")
                result = {
                    "status": "unsuccessful",
                    "primary_endpoint": "url",
                    "sas_token": "",
                    "container_name": "",
                    "blob_name": "",
                    "message" : f"Not really an error, \nACA Jobs Can't Start"
                }

        else:
            status_code = 503
            result = {
                "status": "unsuccessful",
                "primary_endpoint": "url",
                "sas_token": "",
                "container_name": "",
                "blob_name": "",
                "message" : f"Can't Upload, the same file {full_file_path} is already in process"
            }
    except Exception as e:
        status_code = 503
        result = {
            "status": "unsuccessful",
            "primary_endpoint": "url",
            "sas_token": "",
            "container_name": "",
            "blob_name": "",
            "message" : f"Encountered Error with Uploading {e}"
        }
        logger.error(f"error at getblobtoken, error:[{e}]")
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
async def processUploadedFile(blob_req: func.InputStream):
    """ 
    When Files are Uploaded and Completed in Blob, it will add this file in.
        "filename" : filename,
        "file_extension" : file_extention,
        "full_path" : full_path
    """
    try:
        logger.info(f"Python blob trigger function Begin blob. "
                    f"Name: {blob_req.name}, "
                    f"Blob Size: {blob_req.length} bytes")
        full_file_path:str = blob_req.name
        instance_name, file_dict = clean_the_string(full_file_path)
        filename_new = file_dict["filename"]
        
        logger.error("Running")
        # Delete previous uploaded images
        data_storage_manager = TableEntityManager()
        succeeded, rtn_dict = await data_storage_manager.onFileUploadedEvent(filename_new=filename_new)
        # if succeeded:
        #     # Get yo write tokens
        #     pubsub_mng = PubSubManager()
        #     url_write_token = pubsub_mng._get_write_token(instance_name)
            
        #     # Instantiate Containers
        #     container_obj = CreateContainerAppsManager2()
        #     pubsub_vars = {
        #         "PUBSUBGROUPNAME":instance_name,
        #         "PUBSUBURL": url_write_token
        #     }
        #     test_envs = {
        #         "StorageAccName": data_storage_manager.strg_account_name,
        #         "StorageAccKey": data_storage_manager.strg_access_key,
        #         "StorageEndpointSuffix" : data_storage_manager.strg_endpoint_suffix,
        #         "StorageContainer": data_storage_manager.folder_container,
        #         "DBRoot":data_storage_manager.root_log_table_name
        #     }
        #     env_vars_merged = {**test_envs,**pubsub_vars, **rtn_dict}
        #     file_sizeGB = rtn_dict["file_size_gb"]
        #     # complete_config = container_obj.get_complete_execution_config("tph-app-job-aus-east-l2gdwab")
            
        #     # res = container_obj.run_jobv2(file_sizeGB, env_dict=env_vars_merged)
        #     logger.info(f"Successfully Run Container Jobs{res}")
        # else:
        #     logger.warn("Container Jobs Not Running")
        # container_obj.run_job(instance_name, url_write_token, rtn_dict)
    except Exception as e:
        logger.error(f"Create Container Error: {e}")
    

@app.function_name(name="getQueries")
@app.route(route="getQueries/{page?}", methods=["GET"])
def getQueries(req:func.HttpRequest) -> func.HttpResponse:
    try:
        page = req.route_params.get('page', 1)
        page = int(page)
        per_page = 5
        # Init
        items_data = []
        data_storage_manager = TableEntityManager()
        data_list = data_storage_manager.query_db()
        # logger.warn(data_list)
        n_data = len(data_list)
        
        # Pagination
        total_pages = (n_data + per_page - 1) // per_page
        start_index = n_data - (page * per_page)           # From the end
        end_index = n_data - ((page - 1) * per_page)       # To the end

        # Ensure indices are within bounds
        start_index = max(0, start_index)
        end_index = max(0, end_index)
        current_page_items = data_list
        
        logger.warning(f"{start_index}, {end_index}")
        # [id, filename, coordinates, processed_url, status, logs_url, date, size]
        for i in range(end_index, start_index, -1):
            current_data = current_page_items[str(i-1)]
            error = current_data["error"]
            if error == "True"or error == True:
                status = f"{current_data['status']} Error"
            else:
                status = current_data["status"]
            processed_folder = f"{current_data['process_folder']}"
            
            # IF Completed Generate Images
            if ((current_data["error"]     == 'False'  or current_data["error"] == False ) and \
                (current_data["replaced"]  == 'False' or current_data["replaced"] == False )and \
                current_data["completed"] == 'True'
                ):
                # Generate Images
                sasUrlsSide, sasUrlTop  = data_storage_manager.query_image_list(processed_folder)[:6]
                data_storage_manager.blob_obj.generate_sas_Container_token(read_only=True)
                primary_endpoint, sas_token, container_name = data_storage_manager.blob_obj.generate_sas_Container_token(read_only=True)
            else:
                sasUrlsSide, sasUrlTop = [], ""
                logger.debug(f"{current_data['error']}, {bool(current_data['replaced'])}, {current_data['completed']}")
                primary_endpoint, sas_token, container_name = "","",""
            logger.warning(f"{current_data['error']}, {current_data['replaced']}, {current_data['completed']}, Error : [{current_data['error_msg']}]")
            ## Ensure ALL VALUES ARE POPULATED
            items_data.append({
                "id"            : str(i),
                "filename"      : f"{current_data['RowKey']}{current_data['ext']}",
                "upload_loc"    : f"{current_data['file_upload_full_path']}",
                "coordinates"   : f"{current_data['coordinates']}",
                "processed_url" : f"{current_data['process_folder']}",
                "status"        : status.title(),
                "logs_url"      : f"{current_data['log_file']}",
                "date"          : current_data['upload_starttime'], #f"{time_human_readable(current_data['upload_starttime'])}",
                "size"          : f"{current_data['trees_completed']}/{current_data['file_size_gb']:.1f}",
                "error"         : error,
                "sas_token": { # For Downloading Blob
                    "endpoint": primary_endpoint,
                    "sas_token" : sas_token,
                    "container_name" : container_name
                    },
                "sideImages"    : sasUrlsSide,
                "topImage"      : sasUrlTop
                })

        # Build Response
        response_data = {
            "items": items_data,
            "pagination":{
                'current_page': page,
                'total_pages': total_pages,
                'total_items': n_data,
                'items_per_page': per_page,
                'has_previous': page > 1,
                'has_next': page < total_pages
            }
        }
        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logger.error(f"Exception \n\n\n{e}\n\n\n")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )








@app.function_name(name="getDownloadBlobToken")
@app.route(route="getDownloadBlobToken")
def getDownloadBlobToken(req:func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        processed_url = req_body.get('processed_url')
        filename = req_body.get('filename')

        data_storage_manager = TableEntityManager()

        # primary_endpoint, sas_token, container_name = data_storage_manager.blob_obj.generate_sas_Container_token(read_only=True)
        location = os.path.join(processed_url,f"{Path(processed_url).stem}.zip")
        sas_url = data_storage_manager.generate_sas_url_for_blob(location)
        status_code = 200
        result = {
            "status": "success",
            "sas_token": sas_url,
            "message" : "Here is your token"
        }
    except Exception as e:
        status_code = 503
        result = {
            "status": "unsuccessful",
            "upload_url": "url",
            "sas_token": "",
            "container_name": "",
            "message" : f"Encountered Error with Uploading {e}"
        }
        logger.error(f"error at getblobtoken, error:[{e}]")
    return func.HttpResponse(
        json.dumps(result),
        status_code=status_code,
        mimetype="application/json"
    )