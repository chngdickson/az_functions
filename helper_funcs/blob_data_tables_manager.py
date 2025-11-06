import os
import asyncio 
from uuid import uuid4, UUID
from copy import copy
from datetime import datetime,timezone, timedelta
from dotenv import find_dotenv, load_dotenv
from typing_extensions import TypedDict
from azure.data.tables import TableClient, TableServiceClient, UpdateMode
from azure.storage.blob import BlobServiceClient, generate_blob_sas, generate_container_sas, BlobSasPermissions, ContainerSasPermissions
from azure.core.exceptions import ResourceNotFoundError, ResourceModifiedError
from .helper import get_time_now, clean_the_string, process_expired
from typing import Tuple
import random
"""
My frontend does not need to communicate with the Database Table Storage
Frontend tells function app - I want to Upload to the Cloud
Backend says 
    
    func get_return_token # Return Token
    - Okay! Let me check if u can upload, 
        - if can_upload
            [return token]
        - else
            [return nothing]
    
    func query_db
        - find file_list that are (completed == False)
            -- if there are files that are not completed
                - for each file
                    - func process_is_running
        [return queries]
    
    func can_upload
        - if rootfile don't exist
            - add new PartitionKey
        [Return True]
        
        - if filename exist, 
            - if completed = True
                -- if replaced = True, 
                    --- add new PartitionKey 
                    [Return True]
                -- else 
                    --- update_replace
                    [Return True]
            - else 
                -- if process_is_running 
                    [Return False]
                -- else 
                    --- add new PartitionKey
                    [Return True]
    
    func update_replace
        - if replaced = True
            [Return True]
        - else
            -- Update replace
            -- remove process_folder
            -- clean log_filename.db
    
    func process_is_running
        - if error_msg = empty, do the following
            - if upload_completed = False, update error, error_msg [Return False]
            - if process_completed = False or store_completed = False, 
                -- if process_starttime exceeded, update error, error_msg [Return False]
                -- else [Return True]
    
    func eventStart
        - find filename
        - update upload_completed
"""
class RootDataTable(TypedDict, total=False):
    PartitionKey: str           # ID                    # Passed to Container
    RowKey: str                 # Filename              # Passed to Container
    ext: str                    # File Extension
    file_size_gb: float         # X GB
    file_upload_full_path: str  # Blob pcdUploads/filename.laz
    process_folder:str          # Blob processed/foldername
    log_root: str               # Blob logs/log_root.csv
    log_file: str               # Blob logs/log_filename.csv 
    upload_completed:str        # [True, False]             
    process_completed:str       # [True, False]
    store_completed:str         # [True, False]
    status:str                  # [upload, process, store, completed, replaced] 
    # Edited by Container or Function App
    coordinates:str
    upload_starttime:str
    process_starttime:str       # 
    process_expiretime:str      #
    replaced:str                # [True, False]
    completed:str               # [True, False]        
    error:str                   # [True, False]
    error_msg:str               # [Upload not completed]
    # Function App
    trees_completed : int       # [Number of Trees]
    

class Blob_Manager():
    def __init__(self, conn_string, container_name, blob_expiry_time:int):
        self.conn_string = conn_string
        self.container_name = container_name
        self.blob_expiry_time = blob_expiry_time
        self.blob_service = BlobServiceClient.from_connection_string(conn_string)
        self.container_obj = self._check_container_exist(container_name)
        self.blob_account_key = self.blob_service.credential.account_key
        self.blob_account_name = self.blob_service.account_name
        self.blob_primary_endpoint = self.blob_service.primary_endpoint
        
    def _check_container_exist(self, container_name):
        # ✅ Ensure container exists, if not exist create one
        container = self.blob_service.get_container_client(container_name)
        try:
            container.get_container_properties()
        except ResourceNotFoundError:
            container.create_container()
        return container
    
    
    def generate_sas_file_token(self, upload_location:str, read_only:bool):
        if read_only:
            perm = BlobSasPermissions(read=True)
        else:
            perm = BlobSasPermissions(write=True, create=True, add=True)
        try:
            container_name = self.container_name
            primary_endpoint = self.blob_primary_endpoint
            sas_token = generate_blob_sas(
                account_name=self.blob_account_name,
                container_name=container_name,
                blob_name= upload_location,
                account_key=self.blob_account_key,
                permission=perm,
                expiry = datetime.now(timezone.utc) + timedelta(minutes=self.blob_expiry_time)
            )
        except Exception as e:
            print(f"Error at [Generate_sas_file_token] with error: {e}")
        return primary_endpoint, sas_token, container_name, upload_location
    
    def generate_sas_Container_token(self, read_only=False):
        try:
            if read_only == True:
                # perms = ContainerSasPermissions(write=True, create=True, add=True, read=True, list=True)
                perms = ContainerSasPermissions(read=True, list=True)
            else:
                perms = ContainerSasPermissions(write=True, create=True, list=True)
                perms = ContainerSasPermissions(write=True, create=True, add=True, read=True, list=True)
            sas_token = generate_container_sas(
                account_name=self.blob_account_name,
                container_name=self.container_name,
                account_key=self.blob_account_key,
                permission=perms,
                expiry = datetime.now(timezone.utc) + timedelta(minutes=self.blob_expiry_time)
            )
            primary_endpoint = self.blob_primary_endpoint
            container_name = self.container_name
        except Exception as e:
            print(f"Error at [generate_sas_Container_token] with error: {e}")
        return primary_endpoint, sas_token, container_name
    
class TableEntityManager(object):
    ####========================================================= ###
    ####================= END: Init ============================= ###
    ####========================================================= ###
    def __init__(self):
        load_dotenv(find_dotenv())
        """ Env Files """
        self.strg_account_name    = str(os.environ["StorageAccName"])
        self.strg_access_key      = str(os.environ["StorageAccKey"])
        self.strg_endpoint_suffix = str(os.environ["StorageEndpointSuffix"])
        
        self.connection_string      = f"DefaultEndpointsProtocol=https;AccountName={self.strg_account_name};AccountKey={self.strg_access_key};EndpointSuffix={self.strg_endpoint_suffix}"
        
        self.root_log_table_name= str(os.getenv("DBRoot", "rootLog"))
        self.folder_container   = str(os.getenv("FolderContainer", "tph-files"))
        self.folder_logs        = str(os.getenv("FolderLog", "logs"))
        self.folder_uploads     = str(os.getenv("FolderUpload", "pointcloudUploads"))
        self.folder_processed   = str(os.getenv("FolderProcessed", "processed"))
        self.timeout_blob       = int(os.getenv("Timeout_Blob_mins", "10"))
        self.timeout_pubsub     = int(os.getenv("PubSub_minutes_to_expire","10"))
        self.image_loc          = str(os.getenv("saveInSideView","sideView"))
        self.create_database_if_not_exists()
        self.blob_obj = self.init_blob_manager()
        
    def create_database_if_not_exists(self):
        # [START create_table_if_not_exists]
        try:
            with TableServiceClient.from_connection_string(self.connection_string) as table_service_client:
                table_client = table_service_client.create_table_if_not_exists(table_name=self.root_log_table_name)
                print(f"Table name: {table_client.table_name}")
                return table_client
        except Exception as e:
            print(f"Error occured in [create_database_if_not_exist] {e}")
    
    def init_blob_manager(self):
        return Blob_Manager(self.connection_string, self.folder_container, self.timeout_blob)
    
    def add_new_partition(self, full_file_path:str, file_size_gb:float):
        """

        Args:
            full_file_path (str): folder/filename_f23.txt
            file_size_gb (float): 10.4

        Returns:
            _type_: _description_
        """
        try:
            instance_name, file_dict = clean_the_string(full_file_path)
            filename_new = str(file_dict["filename"])
            file_extention = str(file_dict["file_extension"])
            
            pcd_upload_path = f"{self.folder_uploads}/{filename_new}{file_extention}"
            process_folder = f"{self.folder_processed}/{filename_new}/"
            log_root_pth = f"{self.folder_logs}/root.csv"
            log_file_pth = f"{self.folder_logs}/{filename_new}_log.csv"
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=self.root_log_table_name) as table_client:
                root_init_dict: RootDataTable ={
                    # Init
                    "PartitionKey": str(self.__len__()),         # ID - Passed to Container
                    "RowKey": filename_new,                      # Filename - Passed to Container
                    "ext": file_extention,                       # File Extension
                    "file_size_gb": file_size_gb,                # X GB
                    "file_upload_full_path": pcd_upload_path,    # Blob pcdUploads/filename.laz
                    "process_folder": process_folder,            # Blob processed/foldername
                    "log_root": log_root_pth,                    # Blob logs/log_root.csv
                    "log_file": log_file_pth,                    # Blob logs/log_filename.csv 
                    "upload_completed": str(False),              # [True, False]             
                    "process_completed": str(False),             # [True, False]
                    "store_completed": str(False),               # [True, False]
                    "status": "upload",                          # [upload, process, store, completed, replaced]
                    # Not initiated here
                    "coordinates":"X",
                    "upload_starttime": get_time_now(0),
                    "process_starttime": "",                     # str in isoformat
                    "process_expiretime": "",                    # str in isoformat
                    "replaced": str(False),                           # [True, False]
                    "completed": str(False),                     # [True, False]        
                    "error": str(False),                         # [True, False]
                    "error_msg": "",                             # [Upload not completed]
                    "trees_completed": 0                         # [Number of Trees]
                }
                table_client.create_entity(entity=root_init_dict)
            return True
        except Exception as e:
            print(f"Error occured in [add_new_partition] {e}")
            return False
    ####========================================================= ###
    ####================= END: Init ============================= ###
    ####========================================================= ###
    
    ####========================================================= ###
    ###============ Start: Upload Functions ===================== ###
    ####========================================================= ###
    def upload_file_infos(self, full_file_path:str):
        if not isinstance(full_file_path,str):
            full_file_path = str(full_file_path)
        instance_name, file_dict = clean_the_string(full_file_path)
        filename_new = file_dict["filename"]
        filename_ext = file_dict["file_extension"]
        pcd_upload_path = f"{self.folder_uploads}/{filename_new}{filename_ext}"
        return self.blob_obj.generate_sas_file_token(pcd_upload_path, read_only=False)
        
    def can_upload(self, full_file_path:str, file_size_gigabytes:float):
        """If it's possible to upload, add a partition and sends True

        Args:
            full_file_path (str): _description_
            file_size_gigabytes (float): _description_

        Returns:
            _type_: _description_
        """
        try:
            instance_name, file_dict = clean_the_string(full_file_path)
            filename_new = file_dict["filename"]
            permission_to_upload = False
            if self.process_is_running(filename_new):
                permission_to_upload = False
            else:
                permission_to_upload = True
                self.update_replace_upon_upload(filename_new)
                self.add_new_partition(full_file_path, file_size_gigabytes)
                
            return permission_to_upload
        except Exception as e:
            print(f"Error occured in [can_upload], {e}")
            return permission_to_upload 
    
    def update_replace_upon_upload(self, filename_new:str):
        try:
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=self.root_log_table_name) as table_client:
                entities_to_replace = self.query_table_by_key_value(
                    ["RowKey","upload_completed","error","replaced"],
                    [filename_new, True, False, False]
                    )
                for replace_entity in entities_to_replace:
                    replace_entity["status"] = "replaced"
                    replace_entity["replaced"] = str(True)
                    replace_entity["error_msg"] = "This File has been replaced by user"
                    table_client.upsert_entity(mode=UpdateMode.MERGE, entity=replace_entity)
                    
        except Exception as e:
            print(f"Error occured in [update_replace_upon_upload], {e}")
        return None
    ####================================================================== ###
    ####================= END: Upload Functions ========================== ###
    ####================================================================== ###
    
    
    ####================================================================== ###
    ####=========== Start : Event Process Functions ====================== ###
    ####================================================================== ###
    async def onFileUploadedEvent(self, filename_new):
        # TODO: ERROR
        file_list = []
        rtn_dict = {}
        try:
            print(f"{filename_new}\n\n\n")
            # 1. Update Upload Completion
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=self.root_log_table_name) as table_client:
                entities_to_update = self.query_table_by_key_value(
                    ["RowKey","completed","upload_completed","error","replaced","status"],
                    [filename_new, False, False, False, False,"upload"]
                    )
                file_list = copy(entities_to_update)
                for entity in entities_to_update:
                    print(entity)
                if len(file_list) == 1:
                    for entity in entities_to_update:
                        update_entity = file_list[0]
                        update_entity["status"] = "process"
                        update_entity["upload_completed"] = str(True)
                        update_entity["process_starttime"] = get_time_now(0)
                        update_entity["process_expiretime"] = get_time_now(self.timeout_pubsub)
                        table_client.upsert_entity(mode=UpdateMode.MERGE, entity=update_entity)
                        
                        
                        process_folder = update_entity["process_folder"]                    
                        key_val_to_copy = ["PartitionKey","RowKey","log_root","log_file",
                                        "process_folder","file_upload_full_path","ext"]
                        for key, value in update_entity.items():
                            if key in key_val_to_copy:
                                rtn_dict[key] = value
                        rtn_dict["file_size_gb"]=update_entity["file_size_gb"]
                    await self.delete_process_folder_on_pcd_upload(self.folder_container, process_folder)
                        
                    
                    return True, rtn_dict
                elif len(file_list) > 1:
                    for update_entity in file_list:
                        update_entity["error"] = str(True)
                        update_entity["error_msg"] = f"There's an additional file while uploading, Possible Clash"
                    return False, {}
                else:
                    return False, {}
            
        except Exception as e:
            print(f"Error occured in [onFileUploadedEvent], {e}")
            return False, {}
    
    ## Azure hmm... Not going to tell us there's no folder? Hmmmmmm?
    async def delete_process_folder_on_pcd_upload(self, container_name, foldername):
        from azure.storage.blob.aio import BlobServiceClient
        try:
            blob_service = BlobServiceClient.from_connection_string(self.connection_string)
            
            async with blob_service.get_container_client(container_name) as blob_client:
                deletingBlobs=[]
                async for blob in blob_client.list_blobs():
                    if blob.name.startswith(foldername):
                        deletingBlobs.append(blob_client.delete_blob(blob.name, delete_snapshots="include"))
            await asyncio.gather(*deletingBlobs)
            await blob_service.close()
            return True
            
        except Exception as e:
            print(f"Error occured in [delete_process_folder_on_pcd_upload], [{e}]")
            return False
            
    def process_is_running(self, filename_new)-> bool:
        """if any process is still running, you're not allowed to upload

        Args:
            filename (_type_): _description_

        Returns:
            bool: True if any process is still running
        """
        in_progress_entity = []
        try:
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=self.root_log_table_name) as table_client:
                
                # 1. Update is not completed, Init Error
                entities = self.query_table_by_key_value(["RowKey","upload_completed","error"],[filename_new,False,False])
                for entity in entities:
                    entity["error"] = True
                    entity["error_msg"] = "Upload Failed, Webpage Exited when uploading"
                    table_client.update_entity(mode=UpdateMode.MERGE, entity=entity)
                    print(f"Update entity is ... {entity}")
                
                # 2. Process not completed
                maybe_in_progress_entities = self.query_table_by_key_value(
                    ["RowKey","process_completed","completed","error"],
                    [filename_new, True, False, False]
                    )
                
                for progress_entity in maybe_in_progress_entities:
                    process_expiretime = progress_entity["process_expiretime"]
                    
                    if process_expired(process_expiretime):
                        progress_entity["error"] = True
                        progress_entity["error_msg"] = "Process Timeout Exceeded :("
                        table_client.upsert_entity(mode=UpdateMode.MERGE, entity=progress_entity)
                    else:
                        in_progress_entity.append(progress_entity)
                
                if len(in_progress_entity) > 0:
                    # Something is still working
                    return True
                else:
                    return False
        except Exception as e:
            print(f"Error occured in [Process_Is_running], {e}")
            return False
        return False
    ####================================================================== ###
    ####================= End : Process Functions ======================== ###
    ####================================================================== ###
    
    
    ####================================================================== ###
    ####================= Start: Query Functions ========================= ###
    ####================================================================== ###
    
    def query_db(self)->dict:
        result = []
        try:
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=self.root_log_table_name) as table_client:
                uploadIncompleteEntities = self.query_table_by_key_value(["upload_completed","error"],[False,False])
                for uploadIncomplete in uploadIncompleteEntities:
                    uploadIncomplete["error"] = str(True)
                    uploadIncomplete["error_msg"] = "Upload Failed, Webpage Exited when uploading"
                    table_client.upsert_entity(mode=UpdateMode.MERGE, entity=uploadIncomplete)
                processEntities = self.query_table_by_key_value(["process_completed","error"],[False, False])
                for processEntity in processEntities:
                    print("process_completed", processEntity)
                    if processEntity["process_expiretime"] < get_time_now(0):
                        processEntity["error"] = str(True)
                        processEntity["error_msg"] = "Processing Failed, Processing Ran out of time"
                        table_client.upsert_entity(mode=UpdateMode.MERGE, entity=processEntity)
            return self.list_all_entities()
        except Exception as e:
            print(f"Error occured in [query_db], {e}")
            return result
    
    def generate_sas_url_for_blob(self, blob_loc):
        try:
            end_pt, sas_token, container_name, blob_name = self.blob_obj.generate_sas_file_token(blob_loc, read_only=True)
            # sas_url = f"{end_pt}?{sas_token}"
            sas_url = f"{end_pt}{container_name}/{blob_name}?{sas_token}"
            return sas_url
        except Exception as e:
            return ""
        
    def query_image_list(self, process_folder)-> Tuple[list, str]:
        sasUrlsSide = []
        sasUrlTop   = ""
        try:
            folder_sideView = process_folder+self.image_loc
            folder_topView  = process_folder+"topView"
            blobContainerClient = self.blob_obj.blob_service.get_container_client(self.folder_container)
            
            sideViewPaths  = random.sample([blob.name for blob in blobContainerClient.list_blobs(folder_sideView)],6)
            topViewPaths   = [blob.name for blob in blobContainerClient.list_blobs(folder_topView)]
            for im_loc in sideViewPaths:
                sasUrlsSide.append(self.generate_sas_url_for_blob(im_loc))
            
            sasUrlTop = self.generate_sas_url_for_blob(topViewPaths[0])
        except Exception as e:
            # raise IOError("Received IO error when querying Images")
            return sasUrlsSide, sasUrlTop
        return sasUrlsSide, sasUrlTop
    
    ####================================================================== ###
    ####================= End: Query Functions =========================== ###
    ####================================================================== ###
    
    
    ####================================================================== ###
    ####================= Start: Helper Functions ======================== ###
    ####================================================================== ###
    def query_table_by_key_value(self, keys:list=["RowKey","ext"], values:list=["filename",".txt"]):
        try:
            results = []
            if len(keys) != len(values):
                assert f"Keys and Values have different length"
        
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=self.root_log_table_name) as table_client:
                query_string = "" # E.g. "key2 eq 'value1' and key2 eq 'value2'"
                for i, (key, value) in enumerate(zip(keys, values)):
                    if i==0:
                        query_string = query_string+f"{key} eq '{value}'"
                    else:
                        query_string = query_string+f" and {key} eq '{value}'"
                entities = table_client.query_entities(query_string)
                
                for entity in entities:
                    results.append(entity)
                    
            return results
        except Exception as e:
            print(f"Error occured in [query_table_by_key_value] {e}")
            return results
    
    def list_all_entities(self)->dict:
        entity_dict = {}
        try:
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=self.root_log_table_name) as table_client:
                # entities=copy(list(table_client.list_entities()))
                entities = table_client.list_entities()
                for entity in entities:
                    id = entity['PartitionKey']
                    entity_dict[id] = entity
            return entity_dict
        except Exception as e:
            print(f"Error occured in [list_all_entities] {e}")
            return entity_dict
    
    def __len__(self):
        return len(self.list_all_entities())
    

    ####================================================================== ###
    ####================= END: Helper Functions ======================== ###
    ####================================================================== ###
    
if __name__ == "__main__":
    def main():
        sample = TableEntityManager()
        # sample.add_new_partition("folder/myfile.txt",0.5)
        sample.add_new_partition("folder/myfile2.txt",0.5)
        entities = sample.query_table_by_key_value(keys=["RowKey","ext"],values=["myfile",".txt"])
        for entity in entities:
            print(entity)
        print(sample.query_db())
        # entities = sample.query_table_by_key_value(keys=["RowKey","ext"],values=["myfile2",".txt"])
        # for entity in entities:
        #     print(entity)
    main()