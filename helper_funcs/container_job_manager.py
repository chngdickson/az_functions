import os
from .identity_stuff import AzureResourceManager
from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.models import RegistryCredentials, JobExecutionTemplate, Job, JobExecutionContainer, JobConfiguration, JobTemplate,JobConfigurationManualTriggerConfig, Container,ContainerResources
from azure.core.exceptions import HttpResponseError

class CreateContainerAppsManager:    
    def __init__(self):
        self.minutes_to_expire = int(os.getenv("PubSub_minutes_to_expire", "10"))
        self.resource_group_name = os.getenv("ResourceGroupName","myResourceGroup")
        self.location = os.getenv("location","eastasia")
        
        self.registry_name = os.getenv("RegistryName")
        self.registry_login_server = os.getenv("RegistryLoginServer")
        self.registry_password = os.getenv("RegistryPassword")
        self.registry_image_name = os.getenv("RegistryImageName")
        
        self.container_job_name = os.getenv("ContainerJobName","tph-container-job")
        self.container_apps_managed_env_id = os.getenv("ContainerAppsManagedEnvID")
        
        self.sub_id = AzureResourceManager().sub_id
        self.seconds_to_expire = int(self.minutes_to_expire*60)
        self.client = ContainerAppsAPIClient(credential=DefaultAzureCredential(), subscription_id=self.sub_id)
        self.job_params:Job = self._init_job_params()
        
    def _init_job(self, job_params:Job):
        response = self.client.jobs.begin_create_or_update(
            resource_group_name=self.resource_group_name, 
            job_name=self.container_job_name, 
            job_envelope=job_params
            ).result() # This will only make a definition of your Job, it will not run it.
        print(response)
        
    
    def start_container(self, pubsub_groupname, pubsub_url):
        job_execution_template = JobExecutionTemplate(
            containers=[
                JobExecutionContainer(
                    name=pubsub_groupname,
                    image=f"{self.registry_login_server}/{self.registry_image_name}:latest",
                    command=[
                        "--pubsub_groupname", pubsub_groupname,
                        "--pubsub_url", pubsub_url
                    ],
                    resources=ContainerResources(
                        cpu=0.25, 
                        memory="250Mb",
                        ephemeral_storage="1Gi"
                    )
                )
            ]
        )
        try:
            response = self.client.jobs.begin_start(
                resource_group_name=self.resource_group_name, 
                job_name=self.container_job_name, 
                template=job_execution_template).result()
        except Exception as e:
            print("SHEEEEEET")
        print(f"Job {pubsub_groupname} execution started with ID: {response.id}")
        
    def _init_job_params(self)->Job:  
        job_params = Job(
            location=self.location,
            environment_id=self.container_apps_managed_env_id,
            configuration= JobConfiguration(
                trigger_type="Manual",
                replica_timeout=self.seconds_to_expire,
                replica_retry_limit=0,
                manual_trigger_config=JobConfigurationManualTriggerConfig(
                    replica_completion_count=1,
                    parallelism=1
                ),
                registries=[
                    RegistryCredentials(
                        server=self.registry_login_server,
                        username=self.registry_name,
                        password_secret_ref=self.registry_password
                    )
                ],
            ),
            template=JobTemplate(
                containers=[Container(
                    image=f"{self.registry_login_server}/{self.registry_image_name}:latest",
                    command=[
                        "--pubsub_groupname", "your_pubsubGroupname",
                        "--pubsub_url", "wss://example.webpubsub.azure.com/..."
                    ],
                    args = [],
                    env = [],
                    resources=ContainerResources(
                        cpu=0.25, 
                        memory="250Mb",
                        ephemeral_storage="1Gi")
                )]
            )
        )
        return job_params


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

from azure.identity import DefaultAzureCredential
from azure.mgmt.appcontainers import ContainerAppsAPIClient
# client = ContainerAppsAPIClient(credential=DefaultAzureCredential(), subscription_id="xxxxxxxxxxxxxx")

class CreateContainerAppsManager2:    
    def __init__(self):
        self.minutes_to_expire = int(os.getenv("PubSub_minutes_to_expire", "10"))
        self.resource_group_name = os.getenv("ResourceGroupName","myResourceGroup")
        self.location = os.getenv("location","eastasia")
        
        self.registry_name = os.getenv("RegistryName")
        self.registry_login_server = os.getenv("RegistryLoginServer")
        self.registry_password = os.getenv("RegistryPassword")
        self.registry_secret_ref = os.getenv("RegistrySecretRef")
        self.registry_secret_pw = os.getenv("RegistrySecretPw")
        self.registry_image_name = os.getenv("RegistryImageName")
        
        self.container_job_name = os.getenv("ContainerJobName","tph-container-job")
        self.container_apps_managed_env_id = os.getenv("ContainerAppsManagedEnvID")
        
        self.sub_id = AzureResourceManager().sub_id
        self.seconds_to_expire = int(self.minutes_to_expire*60)
        self.client = ContainerAppsAPIClient(credential=DefaultAzureCredential(), subscription_id=self.sub_id)
        self.job_params = self._init_job_params()
        response = self.client.jobs.begin_create_or_update(self.resource_group_name, self.container_job_name, self.job_params).result()
    
    def _init_job_params(self):
        job_parameters = {
            "location": self.location, # Change your location
            "properties": {
                "environmentId": self.container_apps_managed_env_id,
                "workloadProfileName": "Consumption",
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
                        "parallelism": 1
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
                            "name": "testname",
                            "env": [
                                {"name":"PUBSUBGROUPNAME", "value":"groupcontainerxxxx"},
                                {"name":"PUBSUBURL", "value":"url_hello"}
                            ],
                            "resources": {
                                "cpu": 0.25,
                                "memory": "0.5Gi"
                            },
                            "command": [
                                "python", "main.py"
                            ]
                        }
                    ]
                }
            }
        }
        return job_parameters
    
    def run_job(self, pubsub_groupname, pubsub_url):
        val = f"test-1"
        job_execution_template = {
            "containers" : [
                {# You MUST pass the image, known azure bug.
                    "image": f"{self.registry_login_server}/{self.registry_image_name}:latest", 
                    "name": val, # This does not seem to work, it keeps the default name of the Job
                    "env": [ # Can alter all env variables
                        {"name": "PUBSUBGROUPNAME","value": pubsub_groupname},
                        {"name":"PUBSUBURL", "value":pubsub_url}
                        ],
                    "resources": { #You can change this also
                        "cpu": 0.5,
                        "memory": "1Gi",
                    },
                    "command": [
                        "python","main.py"
                    ]
                }
            ]
        }
        # Execute the job
        response = self.client.jobs.begin_start(resource_group_name=self.resource_group_name, job_name=self.container_job_name, template=job_execution_template).result()
        print(f"Job execution started with ID: {response.id}")
