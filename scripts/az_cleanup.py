
"""
This file cleans all data from Azure blob storage container as defined in config
"""

import configparser
from azure.storage.blob import ContainerClient, BlobServiceClient, BlobClient, ContentSettings

config = configparser.ConfigParser()
config.read('config.ini')

AZ_CONN_STR = config['main']['AZ_CONNECTION_STRING']
AZ_CONT_STORAGE = config['main']['AZ_CONTAINER_STORAGE']

## Connect AZ
container_client = ContainerClient.from_connection_string(conn_str=AZ_CONN_STR, container_name=AZ_CONT_STORAGE)
print("[+] Connected to Azure.")

## Delete the container
try:
    container_client.delete_container()
    print("[+] Deleting Container "+AZ_CONT_STORAGE+" .")

except Exception as e:
    print("[-] Container already deleted")

exit
