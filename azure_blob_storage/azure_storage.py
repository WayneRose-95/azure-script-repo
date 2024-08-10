from azure.storage.blob import BlobServiceClient
from azure.storage.blob import BlobClient

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

    # def create_blob_service_client(self, connection_string : str): 
        
    #     return blob_service_client 

    def retrieve_data_from_blob(
            self, 
            connection_string : str,
            container_name : str, 
            blob_name : str, 
            file_name : str 
            ):
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_name = container_name 
        blob_name = blob_name 
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

        print(f"Retrieving {blob_name} from {container_name}")

        download_stream = blob_client.download_blob()
        blob_content = download_stream.readall()
        print(blob_content.decode('utf-8'))

        print('Blob Downloaded successfully')

        with open(file_name, 'w+') as file:
            download_stream = blob_client.download_blob()
            blob_content = download_stream.readall()  
            contents = blob_content.decode('utf-8') 
            file.write(contents) 
        

    # def download_file(self, file_name : str, blob_client : BlobClient): 

    #     with open(file_name, 'w+') as file:
    #         download_stream = blob_client.download_blob()
    #         blob_content = download_stream.readall()  
    #         contents = blob_content.decode('utf-8') 
    #         file.write(contents) 


# account_name = '' # Replace with the name of your azure storage account 
# account_key = '' # Replace with your account key. You can find this under Security + Networking > Access Keys 
# connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
# blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# print("Success")


# container_name = '' # Replace with the name of your container 
# blob_name = '' # Replace with the name of your file. This should contain the file name itself alongside its extension e.g. file.txt
# blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

# print(f"Retrieving {blob_name} from {container_name}")

# download_stream = blob_client.download_blob()
# blob_content = download_stream.readall()
# print(blob_content.decode('utf-8'))

# print("Blob downloaded successfully")

# # Writing the blob to a file 
# # Add the writing mode 
# with open('downloaded_file.txt', 'w+') as file:
#     download_stream = blob_client.download_blob()
#     blob_content = download_stream.readall()  
#     contents = blob_content.decode('utf-8') 
#     file.write(contents) 

