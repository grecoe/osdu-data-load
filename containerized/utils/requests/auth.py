##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
from utils.configuration.configutil import Config
from utils.log.logutil import LogBase, Logger
from azure.identity import ClientSecretCredential

class Credential(LogBase):
    """
    Get the application token using the id and secret from the 
    OSDU deployment key vault. 
    """
    def __init__(self, configuration:Config):
        super().__init__("Credentials", configuration.mounted_file_share_name, configuration.log_identity, True)
        self.configuration = configuration
        self.token = None

    def get_application_token(self) -> str:
        """
        Retrieves an authentication token for the given client/secret pair
        saved in the class parameters. 

        Retrieves it only once, future calls get the same token. 
        """

        logger:Logger = self.get_logger()

        if not self.token:
            try:
                app_scope = self.configuration.platform_client + "/.default openid profile offline_access"
        
                creds = ClientSecretCredential(
                    tenant_id=self.configuration.platform_tenant, 
                    client_id=self.configuration.platform_client, 
                    client_secret=self.configuration.platform_secret
                )
                access_token = creds.get_token(app_scope)

                self.token = access_token.token
            except Exception as ex:
                logger.error(f"Exception acquiring token: {str(ex)}")
                raise ex

        return self.token