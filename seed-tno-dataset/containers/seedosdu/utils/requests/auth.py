##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
from utils.configuration.config import Config
from azure.identity import ClientSecretCredential
from utils.logutil import LogBase, Logger

class Credential(LogBase):
    """
    Get the application token using the id and secret from the 
    OSDU deployment  key vault. 
    """
    def __init__(self, configuration:Config):
        super().__init__("Credentials", configuration.file_share_mount, configuration.log_identity)
        self.configuration = configuration
        self.token = None

    def get_application_token(self) -> str:

        logger:Logger = self.get_logger()

        if not self.token:
            try:
                app_scope = self.configuration.appId + "/.default openid profile offline_access"
        
                creds = ClientSecretCredential(
                    tenant_id=self.configuration.tenant, 
                    client_id=self.configuration.appId, 
                    client_secret=self.configuration.appCred
                )
                access_token = creds.get_token(app_scope)

                self.token = access_token.token
            except Exception as ex:
                logger.error(f"Exception acquiring token: {str(ex)}")
                raise ex

        return self.token