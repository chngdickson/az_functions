import os
from .identity_stuff import AzureIdentityManager
from .helper import clean_the_string
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.models import RegistryCredentials, JobExecutionTemplate, Job, JobExecutionContainer, JobConfiguration, JobTemplate,JobConfigurationManualTriggerConfig, Container,ContainerResources
from azure.core.exceptions import HttpResponseError
from typing import Optional, Tuple
from azure.core.polling import LROPoller
import json
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# az containerapp job show \
#   --name tphcontainerjob \
#   --resource-group testResourcev2

# az containerapp job create \
#   --name tphcontainerjob \
#   --resource-group testResourcev2 \
#   --image gregergergerger.azurecr.io/hellodocker:latest \
#   --trigger-type Manual \
#   --cpu 1 --memory 1Gi 
# az containerapp env show \
#     --name "managedEnvironment-testResourcev2-9d5f" \
#     --resource-group testResourcev2

# az containerapp job start \
#   --name tph-container-job \
#   --resource-group testResourcev2 
#   --command []

# az containerapp job execution list -n tph-container-job -g testResourcev2
# az containerapp job stop -n tph-container-job -g testResourcev2



# az containerapp job logs show -n tph-container-job -g testResourcev2 --container tph-container-job-kaex312

# export token_url=""
# export group_name=""
# az containerapp update \
#     --name tph-container-job \
#     --resource-group testResourcev2 \
#     --override-command "python main.py --pubsub_groupname $group_name --pubsub_url $token_url"

from azure.mgmt.appcontainers import ContainerAppsAPIClient
# client = ContainerAppsAPIClient(credential=DefaultAzureCredential(), subscription_id="xxxxxxxxxxxxxx")

class CreateContainerAppsManager2:    
    def __init__(self):
        self.minutes_to_expire = int(os.getenv("Timeout_Blob_mins", "10"))
        self.resource_group_name = os.getenv("ResourceGroupName","myResourceGroup")
        self.location = os.getenv("location","Australia East")
        
        self.registry_name = os.getenv("RegistryName")
        self.registry_login_server = os.getenv("RegistryLoginServer")
        self.registry_secret_ref = os.getenv("RegistryName")
        self.registry_secret_pw = os.getenv("RegistrySecretPw")
        self.registry_image_name = os.getenv("RegistryImageName")
        self.local_registry_image_name = os.getenv("RegistryImageLocalName")
        
        self.containerInstanceName=os.getenv("ContainerInstanceName")
        self.container_job_name = os.getenv("ContainerAppJobName")
        self.container_apps_managed_env_id = os.getenv("ContainerAppsManagedEnvID")
        
        self.identity:AzureIdentityManager = AzureIdentityManager()
        self.credential = self.identity.credential
        self.sub_id:Optional[str] = self.identity.sub_id
        
        self.seconds_to_expire = int(self.minutes_to_expire*60)
        self.client:Optional[ContainerAppsAPIClient] = self._initclient()
    
    def _initclient(self)-> Optional[ContainerAppsAPIClient]:
        try:
            logger.warning("ACA INit Client Success!")
            return ContainerAppsAPIClient(credential=self.credential, subscription_id=self.sub_id)
        except Exception as e:
            logger.error("ACA INit Client FAILED!")
            return None   
    
    def print_job_executions(self):
        if not self.client:
            return 
        job_execution= self.client.job_execution(
            resource_group_name=self.resource_group_name,
            job_name=f"{self.container_job_name}",
            job_execution_name="tph-app-job-aus-east-l2gdwab"
        )
        template = job_execution.template.as_dict()
        for key, value in template.items():
            logger.warning(f"{key}: {value}")
        logger.warning(job_execution)
    
    def print_job_template(self):
        if not self.client:
            return 
        job= self.client.jobs.get(
            resource_group_name=self.resource_group_name,
            job_name=f"{self.container_job_name}"
        )
        job_json = json.dumps(job.as_dict(), indent=2, default=str)
        logger.error(job_json)
        template = job.configuration.as_dict()
        for key, value in template.items():
            try:
                inner_dict = value.as_dict()
                for k_inner, v_inner in inner_dict.items():
                    logger.warning(f"{k_inner}: {v_inner}")
            except Exception as e:
                logger.warning(f"{key}: {value}")
        logger.warning(job)
        
    def run_job(self, pcd_filesize_in_GB, env_dict:Optional[dict]=None):
        local_testing = os.getenv("LocalTesting")
        
        if local_testing is not None and local_testing == "True":
            logger.warning(f"Starting Docker JOBS")
            return self.run_docker_container(pcd_filesize_in_GB, env_dict)
        else:
            logger.warning(f"Starting ACA JOBS")
            return self.run_ACA_JOBS(pcd_filesize_in_GB,env_dict)
            
    def run_ACA_JOBS(self, pcd_filesize_in_GB, env_dict:Optional[dict]=None)-> Tuple[bool, Optional[LROPoller]]:
        ram = pcd_filesize_in_GB*2 if pcd_filesize_in_GB*2 > 10 else 10
        ram = int(round(ram))
        strg_size = pcd_filesize_in_GB*2.5 if pcd_filesize_in_GB*2 > 10 else 10
        strg_size = int(round(strg_size))
        strg_size = int(16)
        try:
            az_env_list_dict = []
            if env_dict:
                for k,v in env_dict.items():
                    az_env_list_dict.append(
                        {"name":k,"value":v}
                    )
            job_execution_template = {
                "containers" : [
                    {# You MUST pass the image, known azure bug.
                        "image":  f"{self.registry_login_server}/{self.registry_image_name}:latest", 
                        "name": self.containerInstanceName, # This does not seem to work, it keeps the default name of the Job
                        "env": az_env_list_dict,
                        "resources": { #You can change this also
                            "cpu": 8,
                            "memory": f"{ram}Gi",
                            "ephemeralStorage": f"{strg_size}Gi"
                        },
                        "command": [
                            "python3","main2.py"
                        ]
                    }
                ]
            }
            
            result = self.client.jobs.begin_start(
                resource_group_name=self.resource_group_name, 
                job_name = self.container_job_name, 
                template=job_execution_template).result()
            logger.warning(f"Job execution started with ID: {result.id}")
            return True, result
        except Exception as e:
            logger.error(f"Exception occured in run_ACA_JOBS, \nError : [{e}]")
            return False, None
    
    def run_docker_container(self, pcd_filesize_in_GB, env_dict:Optional[dict]=None)-> Tuple[bool, Optional[LROPoller]]:
        # Do NOT ADD Commands. Azure don't like that.
        import subprocess
        """ # On Server only
        "-p","100:8080",
        """
        image_name = self.local_registry_image_name
        cmd = ["docker", "run", "-it","--rm", "--privileged",
            "--net=host","--gpus","all"]
        
        # Add environment variables if specified
        if env_dict:
            for key, value in env_dict.items():
                cmd.extend(["-e", f"{key}={value}"])
        
        # Add image name
        cmd.append(image_name)
        
        # Run the command
        try:
            result = subprocess.Popen(
                cmd
            )
            
            if result.stderr:
                print("STDERR:", result.stderr)
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            print(f"Error running docker: {e}")
            print("STDERR:", e.stderr)
            return False, None
        
    def acaJobsStarted(self, pubsub_mng, data_storage_manager, full_file_path, db_init_dict):
        logger.warning("Starting Job Param")
        instance_name, file_dict = clean_the_string(full_file_path)
        url_write_token = pubsub_mng._get_write_token(instance_name)
        
        var_needed = ["PartitionKey","RowKey","log_root","log_file","process_folder","file_upload_full_path","ext"]
        pubsub_vars = {
                    "PUBSUBGROUPNAME":instance_name,
                    "PUBSUBURL": url_write_token
                }
        storage_vars = {
            "StorageAccName":           data_storage_manager.strg_account_name,
            "StorageAccKey":            data_storage_manager.strg_access_key,
            "StorageEndpointSuffix" :   data_storage_manager.strg_endpoint_suffix,
            "StorageContainer":         data_storage_manager.folder_container,
            "DBRoot":                   data_storage_manager.root_log_table_name
        }
        db_dict = {}
        for key, value in db_init_dict.items():
            if key in var_needed:
                db_dict[key] = value
        db_dict["file_size_gb"] = db_init_dict["file_size_gb"]
        db_dict["DOWNLOAD_WAIT_TIME_MINS"] = int(round(int(os.getenv("Timeout_Blob_mins"))/2))
        
        env_vars_merged = {**pubsub_vars, **storage_vars, **db_dict}

        job_success, result = self.run_job(float(db_dict["file_size_gb"]), env_dict=env_vars_merged)
        return job_success

    def _init_job_params(self):
        try:
            job_parameters = {
                "location": self.location, # Change your location
                "properties": {
                    "environmentId": self.container_apps_managed_env_id,
                    "workloadProfileName": "NC8as-T4",
                    "configuration": {
                        "secrets": [
                            {
                                "name": self.registry_secret_ref, # Storing my registry password
                                "value": self.registry_secret_pw
                            }
                        ],
                        "triggerType": "Manual",
                        "replicaTimeout": self.seconds_to_expire,
                        "replicaRetryLimit": 0,
                        "manualTriggerConfig": {
                            "replicaCompletionCount": 1,
                            "parallelism": 1,
                            "scale": {
                                "minExecutions": 0,
                                "maxExecutions": 100,
                                "pollingInterval": 30,
                            }
                        },
                        "registries": [
                            {
                                "server": self.registry_login_server,
                                "username": self.registry_name,
                                "passwordSecretRef": self.registry_secret_ref,
                            },
                        ]
                    },
                    "template": {
                        "containers": [
                            {
                                "image": f"{self.registry_login_server}/{self.registry_image_name}:latest",
                                "name": self.containerInstanceName,
                                "env": [
                                    {"name":"PUBSUBGROUPNAME", "value":"groupcontainerxxxx"},
                                    {"name":"PUBSUBURL", "value":"url_hello"}
                                ],
                                "resources": {
                                    "cpu": 0.25,
                                    "memory": "0.5Gi",
                                    "ephemeralStorage": "16Gi"
                                },
                                "command": [
                                    "python3", "main2.py"
                                ]
                            }
                        ]
                    },
                    
                }
            }
            response = self.client.jobs.begin_create_or_update(
                resource_group_name=self.resource_group_name, 
                job_name=self.container_job_name, 
                job_envelope=job_parameters).result()    
            logger.warning(response)
        except Exception as e:
            return