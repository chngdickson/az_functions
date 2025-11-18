import os
from azure.messaging.webpubsubclient import WebPubSubClient, WebPubSubClientCredential
from azure.messaging.webpubsubservice import WebPubSubServiceClient
from azure.messaging.webpubsubclient.models import (
    OnConnectedArgs,
    OnGroupDataMessageArgs,
    OnDisconnectedArgs,
    CallbackType,
    WebPubSubDataType,
)


class PubSubManager():
    def __init__(self):
        self.minutes_to_expire = int(os.environ["Timeout_Blob_mins"])
        self.conn_str_pubsub = os.environ["ConnString_PubSub"]
        self.hubname = os.environ["PubSub_HubName"]
        self.service_client = WebPubSubServiceClient.from_connection_string( # type: ignore
            connection_string=self.conn_str_pubsub, hub=self.hubname
        )
    
    def _get_read_token(self, group_name):
        read_token = self.service_client.get_client_access_token(
            roles=[f"webpubsub.joinLeaveGroup.{group_name}"],
            groups=[f"{group_name}"],
            minutes_to_expire=self.minutes_to_expire
        )["url"]
        return read_token
    
    def _get_write_token(self, group_name):
        read_write_token = self.service_client.get_client_access_token(
            roles=[f"webpubsub.joinLeaveGroup.{group_name}",f"webpubsub.sendToGroup.{group_name}"],
            groups=[f"{group_name}"],
            minutes_to_expire=self.minutes_to_expire
        )["url"]
        return read_write_token
    
    def get_read_tokens(self, group_names):
        pubsub_read_tokens = {}
        for group_name, file_info in group_names.items():
            temp_dict = {"group_name": group_name,
                        "url_token" : self._get_read_token(group_name)}
            merged_dict = temp_dict | file_info
            pubsub_read_tokens[group_name] = merged_dict
        return pubsub_read_tokens
    
    def get_write_tokens(self, group_names):
        pubsub_write_tokens = {}
        for group_name, file_info in group_names.items():
            temp_dict = {"group_name": group_name,
                        "url_token" : self._get_write_token(group_name)}
            merged_dict = temp_dict | file_info
            pubsub_write_tokens[group_name] = merged_dict
        return pubsub_write_tokens
