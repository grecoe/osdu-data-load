from datetime import datetime, timedelta
import typing
import os
from utils.configuration import Storage
from azure.storage.fileshare import (
    ShareDirectoryClient,
    generate_account_sas, 
    ResourceTypes, 
    AccountSasPermissions
)

class StorageUtil:

    @staticmethod
    def get_account_sas(
        storage:Storage, 
        read:bool = False, 
        write:bool = False, 
        list_content:bool = False, 
        create:bool = False, 
        add:bool = False) -> str:

        start = datetime.utcnow()
        storage.account_sas = generate_account_sas(
            account_name= storage.account ,
            account_key= storage.account_key,
            resource_types=ResourceTypes(object=True),
            permission=AccountSasPermissions(
                read=read, 
                write=write, 
                list=list_content, 
                create=create, 
                add=add, 
                update=True,
                process=True,
                delete=True
                ),
            start=start,
            expiry=datetime.utcnow() + timedelta(hours=24),
            protocol="https"
        )

        return storage.account_sas

    @staticmethod
    def get_file_url(storage:Storage, file_name:str) -> str:
        file_url = "https://{}.file.core.windows.net/{}".format(
            storage.account,
            storage.share_name
        )

        file_location = os.path.join(
            file_url, 
            storage.path,
            file_name)

        if "\\" in file_location:
            file_location = file_location.replace("\\", "/")            

        file_location += "?{}".format(storage.account_sas)

        return  file_location

    @staticmethod 
    def get_files(storage:Storage) -> typing.List[typing.Tuple[str, str]]:
        return_uris = []
        
        """Get a list of directory children and files in a directory"""
        parent_dir:ShareDirectoryClient = ShareDirectoryClient.from_connection_string(
            conn_str= storage.connection_str, 
            share_name=storage.share_name, 
            directory_path=storage.path)
        

        content = list(parent_dir.list_directories_and_files())        

        file_url = "https://{}.file.core.windows.net/{}".format(
            storage.account,
            storage.share_name
        )

        if content:
            file_list = [x for x in content if not x.is_directory] 
            for share_file in file_list:
                
                file_location = os.path.join(
                    file_url, 
                    storage.path,
                    share_file.name)

                # Windows path breaks URL pattern
                if "\\" in file_location:
                    file_location = file_location.replace("\\", "/")            
                file_location += "?{}".format(storage.account_sas)

                return_uris.append( (share_file.name, file_location) )
        
        return return_uris