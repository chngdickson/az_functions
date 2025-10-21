import azure.functions as func
from azure.storage.blob import BlobServiceClient, generate_container_sas, BlobSasPermissions
from azure.core.exceptions import ResourceNotFoundError, ResourceModifiedError
import json, os
import time
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
class Blob_List_Manager:
    conn_str = os.environ["ConnString_StoragePcd"]
    container_name = os.environ["Config_Container"]
    json_filename = os.environ["Config_Filename"]
    mins_to_expire = int(os.environ["PubSub_minutes_to_expire"])
    blob_upload_timeout = int(os.environ["Timeout_Blob_mins"])
    def __init__(self):
        self.blob_service = BlobServiceClient.from_connection_string(self.conn_str)
        self.container_obj = self._check_container_exist()
        self.root_key:str = "running_instance_list"
        self.init_json_file = {self.root_key: {}}
        self.blob_account_key = self.blob_service.credential.account_key
        self.blob_account_name = self.blob_service.account_name
        self.blob_primary_endpoint = self.blob_service.primary_endpoint
        self.upload_folder_name = "pointcloudUploads/"
        
        pass
    
    def _check_container_exist(self):
        # Check if file exists, if not exist create one
        container = self.blob_service.get_container_client(self.container_name)
        # ✅ Step 1: Ensure container exists
        try:
            container.get_container_properties()
        except ResourceNotFoundError:
            container.create_container()
        return container
    
    def _check_file_exist(self):
        """Returns the File and Download data

        Returns:
            blob: get the blob_obj for filename
            data: jason data
            etag: Ensure no clash during uplaod
        """
        blob = self.container_obj.get_blob_client(self.json_filename)
        
        if blob.exists():
            download = blob.download_blob()
            data = json.loads(download.readall().decode("utf-8"))
            etag = download.properties.get("etag")       
        else:
            # Create new JSON if not exists
            data:dict = self.init_json_file
            blob.upload_blob(json.dumps(data, indent=2), overwrite=True)
            etag = blob.get_blob_properties().etag
            
        return blob, data, etag
    
    def _upload_blob(self, blob, json_data, etag, MAX_RETRIES = 3):
        successful = True
        for attempt in range(MAX_RETRIES):
            try:
                # Try to Upload
                blob.upload_blob(json.dumps(json_data, indent=2), overwrite=True, if_match=etag)
            except ResourceModifiedError:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(0.5 * (attempt + 1))  # backoff
                else:
                    return not successful
        return successful
    
    def _clean_the_string(self, full_path):
        """
        Args:
            full_path (str): _description_

        Returns:
            instance_name (str): The GroupContainer Name
            file_info_dict (dict): 
                - filename      : my_document
                - file_extension: .pdf
                - full_path     : root_folder/abc/my_document.pdf
                - expire_time   : Saves Expiretime in Isoformat 
        """
        full_path = full_path
        filename = Path(full_path).stem
        file_extention = Path(full_path).suffix
        # Remove the extension and filepaths
        instance_name = re.sub(r'[^a-zA-Z0-9]', '', filename)
        instance_name = ("groupContainer"+instance_name).lower()
        file_info_dict = {
            "filename" : filename,
            "file_extension" : file_extention,
            "full_path" : full_path,
            "expire_time" : (datetime.now(timezone.utc) + timedelta(minutes=self.mins_to_expire)).isoformat()
        }
        return instance_name, file_info_dict
    
    def append(self, full_file_path):
        # Clean the String
        instance_name, file_info_dict = self._clean_the_string(full_file_path)
        blob, json_data, etag = self._check_file_exist()
        
        # Update Dict
        instance_dict = json_data.get(self.root_key,{})
        if instance_name not in instance_dict.keys():
            instance_dict[instance_name] = file_info_dict
        
        # Save to blob storage
        json_data[self.root_key] = instance_dict
        return self._upload_blob(blob, json_data, etag)
    
    def remove(self, instance_name:str):
        # Clean the string
        instance_name, _ = self._clean_the_string(instance_name)
        blob, json_data, etag = self._check_file_exist()
        instance_dict = json_data.get(self.root_key,{})
        
        # Update Dict
        if instance_name in instance_dict.keys():
            instance_dict.pop(instance_name)
        
        # Save to blob storage
        json_data[self.root_key] = instance_dict
        return self._upload_blob(blob, json_data, etag)
        
    def cleanConfig_via_expire_time(self):
        blob, json_data, etag = self._check_file_exist()
        instance_dict = json_data.get(self.root_key,{})
        instances_to_delete = []
        print(instance_dict)
        for instance_name, instance_values in instance_dict.items():
            expiry_time = datetime.fromisoformat(instance_values["expire_time"])
            current_time = datetime.now(timezone.utc)
            if current_time > expiry_time:
                print("Expired")
                print(f"curr time {current_time}")
                print(f"expire time {expiry_time}")
                instances_to_delete.append(instance_name)
        if len(instances_to_delete) > 0:
            for instance_name in instances_to_delete:
                instance_dict.pop(instance_name)
            json_data[self.root_key] = instance_dict
            return self._upload_blob(blob, json_data, etag)
    
    def get_data(self):
        _, json_data, etag = self._check_file_exist()
        instance_dict = json_data.get(self.root_key,{})
        return instance_dict
    
    def generate_sas_token(self):
        sas_token = generate_container_sas(
            account_name=self.blob_account_name,
            container_name=self.container_name,
            account_key=self.blob_account_key,
            permission=BlobSasPermissions(write=True, create=True, list=True),
            expiry= datetime.now(timezone.utc) + timedelta(minutes=2)
        )
        primary_endpoint = self.blob_primary_endpoint
        container_name = self.container_name
        blob_name = self.upload_folder_name
        return primary_endpoint, sas_token, container_name, blob_name