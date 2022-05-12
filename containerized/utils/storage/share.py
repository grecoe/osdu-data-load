import typing
import os
import json
from datetime import datetime, timedelta
from azure.storage.fileshare import (
    ShareServiceClient, 
    ShareFileClient, 
    ShareDirectoryClient,
    generate_account_sas, 
    ResourceTypes, 
    AccountSasPermissions
)
from azure.storage.fileshare._models import FileProperties

class FileDetails:
    def __init__(self):
        self.file_name=None
        self.file_path=None
        self.file_url=None

class FileShareUtil:
    def __init__(self, account_name:str, account_key:str, share_name:str):
        # Storage account with share
        self.account_name = account_name
        # Storage account key 
        self.account_key = account_key
        # File share name
        self.share_name = share_name
        # Connection string to share
        self.connection_str = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net".format(
            self.account_name,
            self.account_key
        )
        # Generic URL for account
        self.file_url = "https://{}.file.core.windows.net/{}".format(
            self.account_name,
            self.share_name
        )

        # The account sas token
        start = datetime.utcnow()
        self.account_sas_token = generate_account_sas(
            account_name=self.account_name,
            account_key=self.account_key,
            resource_types=ResourceTypes(object=True),
            permission=AccountSasPermissions(read=True),
            start=start,
            expiry=datetime.utcnow() + timedelta(hours=24),
            protocol="https"
        )

        # self.service:ShareServiceClient = ShareServiceClient.from_connection_string(conn_str=self.connection_str)

    # Download see this, we need more information
    # https://docs.microsoft.com/en-us/python/api/overview/azure/storage-file-share-readme?view=azure-python

    def create_directory(self, directory_path:str) -> bool:
        """Creates a directory if not there, return if true means something
        was created, otherwise it already existed."""
        path_parts = os.path.split(directory_path)
        base_path = ""
        something_created = False

        current_directories = self._list_directories(base_path)

        if len(path_parts) > 1:
            if not len(path_parts[0]):
                path_parts = path_parts[1:]

        for path in path_parts:
            sub_folders = [x.directory_path for x in current_directories]
            if path not in sub_folders:
                something_created = True
                current_client = ShareDirectoryClient.from_connection_string(
                                    conn_str=self.connection_str, 
                                    share_name=self.share_name, 
                                    directory_path=os.path.join(base_path, path))
                current_client.create_directory()

            base_path = os.path.join(base_path, path)
            current_directories = self._list_directories(base_path)

        return something_created

    def list_files(self, directory:str) -> typing.List[FileDetails]:
        """
        List all of the files in a directory in the file share
        """
        return_list:typing.List[FileDetails] = []
        content = self._list_content(directory)
        if content:
            file_list = [x for x in content if not x.is_directory] 
            for share_file in file_list:
                detail = FileDetails()
                detail.file_name = share_file.name
                detail.file_path = directory
                detail.file_url = os.path.join(
                    self.file_url, 
                    detail.file_path,
                    detail.file_name)
                detail.file_url += "?{}".format(self.account_sas_token)

                # Windows path breaks URL pattern
                if "\\" in detail.file_url:
                    detail.file_url = detail.file_url.replace("\\", "/")
                
                return_list.append(detail)

        return return_list

    def upload_file(self, folder:str, file:str) -> bool:
        raw_file_name = os.path.split(file)[-1]

        client = ShareFileClient.from_connection_string(
            conn_str=self.connection_str, 
            share_name=self.share_name, 
            file_path=os.path.join(folder, raw_file_name)
        )

        call_value = None
        if client:
            with open(file, "rb") as source_file:
                call_value = client.upload_file(source_file)

        return isinstance(call_value, dict)

    def download_file(self, local_folder:str,  folder:str, file:str) -> bool:
        file_client = ShareFileClient.from_connection_string(
            conn_str=self.connection_str, 
            share_name=self.share_name, 
            file_path=os.path.join(folder, file)
        )

        # Make path match the incoming folder as well
        folder_path = os.path.join(local_folder, folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        file_path = os.path.join(folder_path, file)

        with open(file_path, "wb") as file_handle:
            data = file_client.download_file()
            data.readinto(file_handle)

    def _list_directories(self, directory) -> typing.List[ShareDirectoryClient]:
        """Get a list of just directories."""
        return_content:typing.List[ShareDirectoryClient] = []
        content = self._list_content(directory)
        directories = [x.name for x in content if x.is_directory]

        for dir_name in directories:
            return_content.append(
                ShareDirectoryClient.from_connection_string(
                    conn_str=self.connection_str, 
                    share_name=self.share_name, 
                    directory_path=dir_name)
            )
        
        return return_content

    def _list_content(self, directory) -> typing.List[FileProperties]:
        """Get a list of directory children and files in a directory"""
        parent_dir:ShareDirectoryClient = ShareDirectoryClient.from_connection_string(
            conn_str=self.connection_str, 
            share_name=self.share_name, 
            directory_path=directory)
        
        parent_dir.directory_path

        return list(parent_dir.list_directories_and_files())

