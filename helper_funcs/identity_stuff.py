from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient, SubscriptionClient
# from azure.mgmt.compute import ComputeManagementClient
# from azure.mgmt.storage import StorageManagementClient
# from azure.mgmt.monitor import MonitorManagementClient
# from azure.core.exceptions import ResourceNotFoundError


class AzureResourceManager:
    def __init__(self):
        self.subscription_id = self.get_subscription_id()
        self.credential = DefaultAzureCredential()
        if self.subscription_id is None:
            return None
        else:
            self.resource_client = ResourceManagementClient(self.credential, self.subscription_id)
            return self.list_all_resources()
    
    def get_subscription_id(self):
        subscription_id = None
        credential = DefaultAzureCredential()
        subscription_client = SubscriptionClient(credential)

        # List all subscriptions visible to this managed identity
        for sub_cli in subscription_client.subscriptions.list():
            try:
                subscription_id = sub_cli.subscription_id
            except:
                subscription_id = None if subscription_id is not None else subscription_id
        return subscription_id
                
    def list_all_resources(self):
        resource_name = None
        try:
            for resource in self.resource_client.resources.list()[0]:
                print(f" - {resource.name} ({resource.type}) in {resource.location}")
                resource_name = resource.name
            return resource.name
        except:
            return None
    

def main():
    return AzureResourceManager()
    

if __name__ == "__main__":
    main()