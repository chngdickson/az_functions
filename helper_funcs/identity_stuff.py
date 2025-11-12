from azure.core.credentials import TokenCredential
from azure.core.exceptions import ClientAuthenticationError
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
import os
from typing import Optional
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
class AzureIdentityManager:
    def __init__(self):
        client_id = str(os.getenv("ManagedIdentityClientID"))
        self.client_id:Optional[str] = client_id
        self.credential:TokenCredential = self.get_credential(client_id)
        self.sub_id:Optional[str] = self.get_subscription_id()
        
    def get_subscription_id(self):
        try: 
            subscription_client = SubscriptionClient(self.credential)
            sub_cli = subscription_client.subscriptions.list()[0]
            sub_id  =  sub_cli.subscription_id
            logger.warning(f"get_subscription_id Success")
            return sub_id
        except:
            logger.warning(f"get_subscription_id Failed")
            return os.getenv("S_ID")
    
    def get_credential(self, client_id:str)->TokenCredential:
        try:
            credential = ManagedIdentityCredential(client_id=client_id)
            token = credential.get_token("https://management.azure.com/.default")
            logger.warning(f"get_credential Success")
            return credential
        except ClientAuthenticationError as e:
            credential = DefaultAzureCredential()
            logger.error(f"\
                Error at get_credential using client_id [{client_id}]\
                \nReverting to Default\
                \nError is: [{e}]")
            return credential 
    # def list_all_resources(self):
    #     resource_name = None
    #     try:
    #         for resource in self.resource_client.resources.list()[0]:
    #             print(f" - {resource.name} ({resource.type}) in {resource.location}")
    #             resource_name = resource.name
    #         return resource.name
    #     except:
    #         return None    