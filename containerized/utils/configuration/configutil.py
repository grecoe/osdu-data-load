import os
import configparser
import uuid
import typing
from datetime import datetime
from logging import Logger

class Config:
    ENVIRONMENT_VARIABLES = [
        "DATA_SOURCE_ACCOUNT", 
        "DATA_SOURCE_ACCOUNT_SHARE", 
        "DATA_SOURCE_ACCOUNT_KEY",
        "DATA_SOURCE_MAP",
        "RECORD_STORAGE_ACCOUNT",
        "RECORD_ACCOUNT_KEY",
        "RECORD_SHARE_NAME"
    ]

    def __init__(self, ini_file:str):
        
        # Get the INI settings
        if not os.path.exists(ini_file):
            raise Exception("Settings file is invalid - {}".format(ini_file))

        config = configparser.RawConfigParser()
        config.read(ini_file)

        # Used for logging identity so we don't have overlapping logs.
        self.log_identity = str(uuid.uuid1()) 

        # INI Settings
        self.batch_multiplier = config.get("LOAD", "batch_multiplier")
        self.container_count = config.get("LOAD", "container_count")
        self.record_storage_table:str = config.get("LOAD", "storage_table")
        self.record_storage_partition:str = config.get("LOAD", "storage_table_partition")
        self.workload_path = config.get("WORKLOADS", "work_path")
        self.record_metadata_path:str = config.get("WORKLOADS", "meta_path")

        # Platform name, required to make URL's and ACL's if there update INI settings as well
        self.platform_name:str = self._get_environment("DATA_PLATFORM", False)
        self.storage_url = None
        self.file_url = None
        self.legal_url = None
        self.legal_tag = None
        self.acl_owner = None
        self.acl_viewer = None

        if self.platform_name:
            value = config.get("CONNECTION", "storage_url")
            self.storage_url = value.format(self.platform_name)

            value = config.get("CONNECTION", "file_url")
            self.file_url = value.format(self.platform_name)
            
            value = config.get("CONNECTION", "legal_url")
            self.legal_url = value.format(self.platform_name)

            value = config.get("REQUEST", "legal_tag")
            self.legal_tag = value.format(self.platform_name)

            value = config.get("REQUEST", "acl_owner")
            self.acl_owner = value.format(self.platform_name)

            value = config.get("REQUEST", "acl_viewer")
            self.acl_viewer = value.format(self.platform_name)

        # Customer Environment
        self.source_account:str = self._get_environment("DATA_SOURCE_ACCOUNT")
        self.source_account_key:str = self._get_environment("DATA_SOURCE_ACCOUNT_KEY")
        self.source_account_share:str = self._get_environment("DATA_SOURCE_ACCOUNT_SHARE")

        # Experience Lab Environment
        self.record_account:str = self._get_environment("RECORD_STORAGE_ACCOUNT")
        self.record_account_key:str = self._get_environment("RECORD_ACCOUNT_KEY")
        self.record_account_share:str = self._get_environment("RECORD_SHARE_NAME")
        
        # Processing information 
        self.workflow_record = self._get_environment("WORKFLOW_RECORD", False)

        # TODO : Have to get this somewhere
        self.mounted_file_share_name = self._get_environment("FILE_SHARE_NAME", False)
        if not self.mounted_file_share_name:
            self.mounted_file_share_name = "./"

        # Source map
        # path:ext||path:ext....
        self.data_source_map:typing.Dict[str,str] = {}
        source_map = self._get_environment("DATA_SOURCE_MAP")
        if source_map:
            parts = source_map.split("||")
            for part in parts:
                source = part.split(':')
                if len(source) == 2:
                    if source[0] not in self.data_source_map:
                        self.data_source_map[source[0]] = [] 
                    self.data_source_map[source[0]].append(source[1])


    def _get_environment(self, setting:str, required:bool = True) -> str:
        return_value = None

        if setting not in os.environ:
            if required:
                raise Exception("{} not in the environment".format(setting))
        else:
            return_value = os.environ[setting] 

        return return_value

