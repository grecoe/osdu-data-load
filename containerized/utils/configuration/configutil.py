##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import os
import configparser
import uuid
import typing
from datetime import datetime

class Config:
    """
    Configuration object to be used across workloads. Retrieves required settings from
    the environment and an ini file, typically would be the settings.ini in the location. 
    """

    def __init__(self, ini_file:str):
        
        # Get the INI settings
        if not os.path.exists(ini_file):
            raise Exception("Settings file is invalid - {}".format(ini_file))

        config = configparser.RawConfigParser()
        config.read(ini_file)

        # Used for logging identity so we don't have overlapping logs.
        self.log_identity = str(uuid.uuid4()) 

        # INI Settings - Every container should be getting this so it's generic across
        # both load and workflow. 
        self.batch_multiplier = config.get("LOAD", "batch_multiplier")
        self.container_count = config.get("LOAD", "container_count")
        self.record_storage_table:str = config.get("LOAD", "storage_table")
        self.record_storage_partition:str = config.get("LOAD", "storage_table_partition")
        self.workload_path = config.get("WORKLOADS", "work_path")
        self.record_metadata_path:str = config.get("WORKLOADS", "meta_path")

        # Platform name is required on load to build ACL/Legal tag and on workflow 
        # to build up the URI's required for the API calls. 
        self.platform_name:str = self._get_environment("DATA_PLATFORM")
        self.data_partition:str = "{}-opendes".format(self.platform_name)
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
            self.legal_tag = value.format(self.data_partition)

            value = config.get("REQUEST", "acl_owner")
            self.acl_owner = value.format(self.data_partition)

            value = config.get("REQUEST", "acl_viewer")
            self.acl_viewer = value.format(self.data_partition)


        # Auditing acccount is always required. Load uses it to move records and write them,
        # and workflow uses it to update records.
        self.record_account:str = self._get_environment("RECORD_STORAGE_ACCOUNT")
        self.record_account_key:str = self._get_environment("RECORD_ACCOUNT_KEY")
        self.record_account_share:str = self._get_environment("RECORD_SHARE_NAME")
        

        # Required by both processes as the file share is used at a minimum for logging and
        # keeping auditing records (workflowX.json)
        self.mounted_file_share_name = self._get_environment("FILE_SHARE_NAME")

        ## ADDITIONAL FIELDS PICKED UB BY get_xxx_configuration where required.

        # LOAD: Customer Environment - Required only for load as load will generate 
        # SAS tokens on external data set.
        self.source_account:str = None 
        self.source_account_key:str = None 
        self.source_account_share:str = None 

        # LOAD: Source map
        # path:ext||path:ext....
        self.data_source_map:typing.Dict[str,str] = {}

        # WORKFLOW record is ONLY used in the workflow schema  
        self.workflow_record = None # self._get_environment("WORKFLOW_RECORD", False)

        # WORKFLOW required information for generating a token for the OSDU platform
        self.platform_tenant = None
        self.platform_client = None
        self.platform_secret = None

    @staticmethod
    def get_load_configuration( ini_file:str) -> object:
        """
        get_load_configuration : Get settings specific to 
        scanning an Azure Storage File Share

        Parameters:

        ini_file:
            Settings file

        Returns:
            Instance of Config
        """
        return_config = Config(ini_file)

        # Customer Environment - Required only for load as load will generate 
        # SAS tokens on external data set.
        return_config.source_account = return_config._get_environment("DATA_SOURCE_ACCOUNT")
        return_config.source_account_key = return_config._get_environment("DATA_SOURCE_ACCOUNT_KEY")
        return_config.source_account_share = return_config._get_environment("DATA_SOURCE_ACCOUNT_SHARE")

        # Source map is only used to filter the customer file share
        # Format: path:ext||path:ext....
        source_map = return_config._get_environment("DATA_SOURCE_MAP")
        if source_map:
            parts = source_map.split("||")
            for part in parts:
                source = part.split(':')
                if len(source) == 2:
                    if source[0] not in return_config.data_source_map:
                        return_config.data_source_map[source[0]] = [] 
                    return_config.data_source_map[source[0]].append(source[1])
        
        return return_config
    
    @staticmethod
    def get_workflow_configuration(ini_file:str) -> object:
        """
        get_workflow_configuration : Get settings specific to 
        uploading files to OSDU

        Parameters:

        ini_file:
            Settings file

        Returns:
            Instance of Config
        """
        return_config = Config(ini_file)

        # Workflow record is ONLY used in the workflow schema  
        return_config.workflow_record = return_config._get_environment("WORKFLOW_RECORD")

        # Credentials only needed for API calls
        return_config.platform_tenant = return_config._get_environment("PLATFORM_TENANT")
        return_config.platform_client = return_config._get_environment("PLATFORM_CLIENT")
        return_config.platform_secret = return_config._get_environment("PLATFORM_SECRET")


        return return_config

    def _get_environment(self, setting:str, required:bool = True) -> str:
        """
        _get_environment : Get a setting from os.environ

        Parameters:

        setting:
            Name of setting to retrieve
        required:
            If true and value is not present, throws exception.

        Returns:
            dict representing  request headers.
        """
        return_value = None

        if setting not in os.environ:
            if required:
                raise Exception("{} not in the environment".format(setting))
        else:
            return_value = os.environ[setting] 

        return return_value

    def get_headers(self, access_token:str, post:bool = False) -> dict:
        """
        get_headers : For all OSDU transactions

        Parameters:

        access_token:
            Token to be used as authentication
        post:
            Flag to determine whether to add content type or not

        Returns:
        dict representing  request headers.
        """

        correlation_id = 'workflow-%s' % str(uuid.uuid4())

        headers = {
            "Accept" : "application/json",
            "data-partition-id": self.data_partition,
            "Authorization": "Bearer {}".format(access_token),
            "correlation-id": correlation_id
        }

        if post:
            headers["Content-Type"] = "application/json"

        return headers