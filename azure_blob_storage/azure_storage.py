from azure.storage.blob import BlobServiceClient

account_name = '' # Replace with the name of your azure storage account 
account_key = '' # Replace with your account key. You can find this under Security + Networking > Access Keys 
connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

print("Success")


container_name = '' # Replace with the name of your container 
blob_name = '' # Replace with the name of your file. This should contain the file name itself alongside its extension e.g. file.txt
blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

print(f"Retrieving {blob_name} from {container_name}")

download_stream = blob_client.download_blob()
blob_content = download_stream.readall()
print(blob_content.decode('utf-8'))

print("Blob downloaded successfully")

# Writing the blob to a file 
# Add the writing mode 
with open('downloaded_file.txt', 'w+') as file:
    download_stream = blob_client.download_blob()
    blob_content = download_stream.readall()  
    contents = blob_content.decode('utf-8') 
    file.write(contents) 

