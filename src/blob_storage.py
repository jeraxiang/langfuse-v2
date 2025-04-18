import os
from azure.storage.blob import (
    BlobServiceClient,
    ContainerClient,
    BlobClient
)
from azure.core.exceptions import ResourceExistsError
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True, eq=True)
class ClientParamsInput:
    conn_str: str
    container_name: str


class BlobStorageHandler:
    """
    Read and save ymal file from azure blob storage.
    """

    def __init__(self, container_name: str, conn_str: str) -> None:
        """
        Initializes a BlobStorageHandler instance with the provided client parameters.

        This constructor creates an instance of the BlobStorageHandler class and initializes the client_params attribute with the provided ClientParamsInput object.
        The client_params object contains the necessary parameters for connecting to the Azure Blob Storage service, such as the connection string and container name.

        Parameters:
        - client_params (ClientParamsInput): An instance of the ClientParamsInput class, containing the connection string and container name for the Azure Blob Storage service.

        Returns:
        - None: The constructor does not return any value.
        """
        self.client_params = ClientParamsInput(conn_str=conn_str, container_name=container_name)

    def create_client(self) -> BlobServiceClient:
        """
        Creates a BlobServiceClient instance using the provided connection string.

        This function initializes a BlobServiceClient object using the connection string provided in the client_params attribute.
        The BlobServiceClient is used to interact with the Azure Blob Storage service.

        Parameters:
        - self (BlobStorageHandler): The instance of the BlobStorageHandler class.

        Returns:
        - BlobServiceClient: An instance of the BlobServiceClient class, connected to the Azure Blob Storage service.
        """
        return BlobServiceClient.from_connection_string(self.client_params.conn_str)

    def get_container_client(self) -> ContainerClient:
        """
        Retrieves the ContainerClient instance associated with the specified container in the Azure Blob Storage.

        This function uses the BlobServiceClient instance created by the `create_client` method and retrieves the
        ContainerClient instance associated with the container specified in the `client_params` attribute.

        Parameters:
        - self (BlobStorageHandler): The instance of the BlobStorageHandler class.

        Returns:
        - ContainerClient: An instance of the ContainerClient class representing the specified container in the Azure Blob Storage.
        """
        return self.create_client().get_container_client(self.client_params.container_name)

    def get_blob_client(self, file_path: str) -> BlobClient:
        """
        Retrieves the BlobClient instance associated with the specified file path in the Azure Blob Storage.

        This function uses the ContainerClient instance created by the `get_container_client` method and retrieves the
        BlobClient instance associated with the file path specified. The BlobClient instance is used to interact with the
        specific blob in the Azure Blob Storage.

        Parameters:
        - file_path (str): The path of the blob in the Azure Blob Storage. This parameter is required and must be a non-empty string.

        Returns:
        - BlobClient: An instance of the BlobClient class representing the specified blob in the Azure Blob Storage.
        """
        return self.get_container_client().get_blob_client(file_path)

    def blob_exists(self, file_path: str) -> bool:
        """
        Checks if a blob exists in the Azure Blob Storage.

        This function uses the BlobClient instance associated with the specified file path to check if the blob exists in the Azure Blob Storage.
        If the blob exists, the function returns True; otherwise, it returns False.

        Parameters:
        - file_path (str): The path of the blob in the Azure Blob Storage. This parameter is required and must be a non-empty string.

        Returns:
        - bool: True if the blob exists, False otherwise.
        """
        return self.get_blob_client(file_path).exists()

    def read_obj(self, file_path: str) -> str:
        """
        Reads the content of a blob from Azure Blob Storage.

        This function retrieves the blob specified by the provided file_path and returns its content as a string.
        If the blob does not exist, the function will return an empty string.

        Parameters:
        - file_path (str): The path of the blob in the Azure Blob Storage. This parameter is required and must be a non-empty string.

        Returns:
        - str: The content of the blob as a string. If the blob does not exist, an empty string is returned.
        """
        return self.get_blob_client(file_path).download_blob().readall()  # type: ignore

    def download_file(self, from_file_path: str, to_file_path: str) -> bool:
        """
        Downloads a file from Azure Blob Storage to the local system.

        This function checks if the specified blob exists in the Azure Blob Storage. If it does not exist,
        a ResourceExistsError is raised. If the specified local file path already exists, a ValueError is raised.
        Otherwise, the blob is downloaded to the local system.

        Parameters:
        - from_file_path (str): The path of the blob in the Azure Blob Storage.
        - to_file_path (str): The local file path where the blob will be downloaded.

        Returns:
        - bool: True if the file is successfully downloaded, False otherwise.
        """
        if not self.blob_exists(from_file_path):
            raise ResourceExistsError(
                f"The specified blob `{to_file_path}` does not exists.")
        if os.path.isfile(to_file_path):
            raise ValueError(
                f"The specified file `{to_file_path}` already exists.")
        with open(to_file_path, "wb") as f:
            f.write(self.get_blob_client(
                from_file_path).download_blob().readall())
        return True

    def upload_file(self, from_file_path: str, to_file_path: str, overwrite: bool = False) -> BlobClient:
        """
        Uploads a file from the local system to Azure Blob Storage.

        This function opens the specified local file, reads its content, and uploads it to the specified blob path in the Azure Blob Storage.
        If the blob already exists and the `overwrite` flag is set to False, a ResourceExistsError is raised.
        Otherwise, the blob is overwritten.

        Parameters:
        - from_file_path (str): The local file path of the file to be uploaded.
        - to_file_path (str): The path of the blob where the file will be saved.
        - overwrite (bool, optional): A flag indicating whether to overwrite the blob if it already exists. Defaults to False.

        Returns:
        - BlobClient: The BlobClient object representing the uploaded blob.
        """
        with open(from_file_path, "rb") as file_obj:
            return self.save_obj(file_obj, file_path=to_file_path, overwrite=overwrite)
            
    def save_obj(self, file_obj, file_path: str, overwrite: bool = False) -> BlobClient:
        """
        Saves an object to the Azure Blob Storage.

        This function uploads a file to the specified blob path in the Azure Blob Storage.
        If the blob already exists and the `overwrite` flag is set to False, a ResourceExistsError is raised.
        Otherwise, the blob is overwritten.

        Parameters:
        - file_obj (file-like object): The file-like object containing the data to be uploaded.
        - file_path (str): The path of the blob where the data will be saved.
        - overwrite (bool, optional): A flag indicating whether to overwrite the blob if it already exists. Defaults to False.

        Returns:
        - BlobClient: The BlobClient object representing the uploaded blob.
        """
        if not overwrite and self.blob_exists(file_path):
            raise ResourceExistsError(
                f"The specified blob `{file_path}` already exists")
        else:
            return self.get_container_client().upload_blob(
                name=file_path, data=file_obj, overwrite=True
            )

    def delete_obj(self, file_path: str) -> bool:
        """
        Deletes a blob from the Azure Blob Storage.

        Parameters:
        - file_path (str): The path of the blob to be deleted.

        Returns:
        - bool: True if the blob is successfully deleted, False otherwise.
        """
        self.get_blob_client(file_path).delete_blob(delete_snapshots="include")
        
        if self.blob_exists(file_path):
            return False
        else:
            return True
