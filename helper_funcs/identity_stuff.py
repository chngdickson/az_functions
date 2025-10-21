from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
import os

class AzureResourceManager:
    def __init__(self):
        self.sub_id = self.get_subscription_id()
    
    def get_subscription_id(self):
        try: 
            credential = DefaultAzureCredential()
            subscription_client = SubscriptionClient(credential)
            sub_cli = subscription_client.subscriptions.list()[0]
            return sub_cli.subscription_id
        except:
            return os.getenv("S_ID")
                
    # def list_all_resources(self):
    #     resource_name = None
    #     try:
    #         for resource in self.resource_client.resources.list()[0]:
    #             print(f" - {resource.name} ({resource.type}) in {resource.location}")
    #             resource_name = resource.name
    #         return resource.name
    #     except:
    #         return None    