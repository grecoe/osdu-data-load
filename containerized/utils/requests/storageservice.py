##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import requests
import typing
from utils.log.logutil import LogBase, Logger
from utils.configuration.configutil import Config
from utils.requests.retryrequest import RequestsRetryCommand, RetryRequestResponse


class StorageFileVersionResponse:
    """
    Encapsulates the response from requesting a file version from an OSDU
    instances along with the request statistics. 
    """
    def __init__(self, versions:typing.List[str], response:RetryRequestResponse):
        self.versions:typing.List[str] = versions
        self.response:RetryRequestResponse = response

class StorageRequests(LogBase):
    """
    Encapsulates communication to an OSDU Storage Service
    """

    def __init__(self, configuration:Config, access_token:str):
        super().__init__("StorageRequests", configuration.mounted_file_share_name, configuration.log_identity, True)
        self.configuration = configuration
        self.token = access_token

    def get_file_versions(self, file_identifier:str) -> StorageFileVersionResponse:
        """
        Retrieve the file version of a file in OSDU using the file identifier
        """

        logger:Logger = self.get_logger()

        if not file_identifier:
            logger.warn("Cannot get version with empty identifier")
            return None

        url = self.configuration.storage_url + "/records/versions/" + file_identifier
        headers = self.configuration.get_headers(self.token)

        response:RetryRequestResponse = RequestsRetryCommand.make_request(
            requests.get,
            url,
            headers=headers
        )

        if response.attempts > 1:
            message = f"get_file_versions - {response.action} on {file_identifier} attempts : {response.attempts} codes : {response.status_codes}"
            print(message)
            logger.info(message)

        return_value = None
        if RequestsRetryCommand.is_success(response):
            return_value = response.result["versions"]
        else:
            print("Failed to acquire versions - {}".format(file_identifier))
            logger.warn("Failed to acquire versions : C:{} E:{}".format(response.status_code, response.error))

        return StorageFileVersionResponse(return_value, response)

