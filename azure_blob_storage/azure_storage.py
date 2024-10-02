from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient
from azure.storage.blob import ContainerClient
from azure.core.exceptions import ResourceExistsError
from azure.core.exceptions import ResourceNotFoundError

class AzureBlobStorageConnector:
    
    def __init__(
            self, 
            account_name=None,
            account_key=None,

            ):
        self.account_name = account_name
        self.account_key = account_key


    
    def create_connection_string(self):
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={self.account_name};AccountKey={self.account_key};EndpointSuffix=core.windows.net"
        return connection_string

    def create_blob_service_client(self, connection_string : str): 
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        return blob_service_client 
    
    def create_container_client(self, connection_string : str, container_name : str):

        container_client = ContainerClient.from_connection_string(connection_string, container_name)
        return container_client, container_name 
    
    def create_blob_container(self, container_client : ContainerClient, container_name : str): 

        try: 
            container_client.create_container() 
            print(f"Container {container_name} created")

        except ResourceNotFoundError:
            print(f'Container {container_name} not found.')
            raise ResourceNotFoundError 
    
        except ResourceExistsError:
            print(f'Container {container_name }already exists')
            raise ResourceExistsError
        
    def retrieve_container_properties(container_client : ContainerClient):

        try:
            container_properties = container_client.get_container_properties() 
            return container_properties 
        
        except ResourceNotFoundError:
            print('Resource not found.')
            raise ResourceNotFoundError

    def retrieve_blob_client(
            self, 
            blob_service_client : BlobServiceClient,
            container_name : str, 
            blob_name : str
            ):

        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        print(f"Retrieving {blob_name} from {container_name}")
        return blob_client 
    
    def list_blobs_in_container(self, blob_service_client : BlobServiceClient, container_name : str, blob_tags=False):
        container_client = blob_service_client.get_container_client(container=container_name)

        if blob_tags:
            blob_list = container_client.list_blobs(include=['tags'])
            for blob in blob_list:
                print(f"Name: {blob['name']}, Tags: {blob['tags']}")
                
            return blob_list
        else:
            blob_list = container_client.list_blobs()
            for blob in blob_list:
                print(f"Name: {blob.name}")
            return blob_list 

    def upload_blob(container_client : ContainerClient, blob_name : str, blob_data):

        container_client.upload_blob(blob_name, blob_data)

    def retrieve_data_from_blob(
            self, 
            blob_client : BlobClient,
            file_name : str 
            ):
    
        with open(file_name, 'w+') as file:
            download_stream = blob_client.download_blob()
            blob_content = download_stream.readall()  
            contents = blob_content.decode('utf-8') 
            file.write(contents) 

        print(f'Blob Downloaded successfully. Saved as {file_name}')

if __name__ == "__main__":
    azblobconnector = AzureBlobStorageConnector()

