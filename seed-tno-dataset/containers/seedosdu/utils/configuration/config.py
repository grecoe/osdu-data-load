import os
import configparser
from datetime import datetime
from logging import Logger

class Config:
    def __init__(self, ini_file:str):
        
        if not os.path.exists(ini_file):
            raise Exception("Settings file is invalid - {}".format(ini_file))

        config = configparser.RawConfigParser()
        config.read(ini_file)

        self.settings = ini_file

        # Environment
        self.tenant = self._get_environment("AZURE_TENANT")
        self.appId = self._get_environment("EXPERIENCE_CLIENT")
        self.appCred = self._get_environment("EXPERIENCE_CRED")
        self.platformName = self._get_environment("ENERGY_PLATFORM")

        self.file_share_mount = self._get_environment("SHARE_MOUNT")


        # INI 
        self.StorageURL = config.get("CONNECTION", "storage_url").format(self.platformName)
        self.FileURL = config.get("CONNECTION", "file_url").format(self.platformName)     
        #self.LegalURL = config.get("CONNECTION", "legal_url").format(self.platformName)     
        #self.SchemasURL = config.get("CONNECTION", "schemas_url")
        #self.WorkflowURL = config.get("CONNECTION", "workflow_url")
        #self.SearchURL = config.get("CONNECTION", "search_url")

        self.dataPartition = "{}-opendes".format(self.platformName)   
        self.legalTag = config.get("REQUEST", "legal_tag").format(self.dataPartition) 
        self.aclOwner = config.get("REQUEST", "acl_owner").format(self.dataPartition)
        self.aclViewer = config.get("REQUEST", "acl_viewer").format(self.dataPartition)

        self.batch_multiplier = int(config.get("LOAD", "batch_multiplier")) 

        self.log_name = config.get("LOGGING", "log_name")
        self.logger:Logger = None

    def _get_environment(self, setting:str) -> str:
        if setting not in os.environ:
            raise Exception("{} not in the environment".format(setting))

        return os.environ[setting]

    def get_headers(self, access_token:str, post:bool = False) -> dict:
        correlation_id = 'workflow-create-%s' % datetime.now().strftime('%m%d-%H%M%S')

        """
        Get request headers.

        :param RawConfigParser config: config that is used in calling module
        :return: dictionary with headers required for requests
        :rtype: dict
        """
        headers = {
            "Accept" : "application/json",
            "data-partition-id": self.dataPartition,
            "Authorization": "Bearer {}".format(access_token),
            "correlation-id": correlation_id
        }

        if post:
            headers["Content-Type"] = "application/json"

        return headers