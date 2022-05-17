##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import requests
import typing
from utils.logutil import LogBase, Logger
from utils.configuration.config import Config
from utils.requests.retryrequests import RequestsRetryCommand, RetryRequestResponse


class StorageFileVersionResponse:
    def __init__(self, versions:typing.List[str], response:RetryRequestResponse):
        self.versions:typing.List[str] = versions
        self.response:RetryRequestResponse = response

class StorageRequests(LogBase):

    def __init__(self, configuration:Config, access_token:str):
        super().__init__("StorageRequests", configuration.file_share_mount, configuration.log_identity)
        self.configuration = configuration
        self.token = access_token

    def get_file_versions(self, file_identifier:str) -> StorageFileVersionResponse:

        logger:Logger = self.get_logger()

        if not file_identifier:
            logger.warn("Cannot get version with empty identifier")
            return None

        url = self.configuration.StorageURL + "/records/versions/" + file_identifier
        headers = self.configuration.get_headers(self.token)

        response:RetryRequestResponse = RequestsRetryCommand.make_request(
            requests.get,
            url,
            headers=headers
        )

        if response.attempts > 1:
            message = f"get_file_versions - {response.action} on {file_identifier} attempts : {response.attempts}"
            print(message)
            logger.info(message)

        return_value = None
        if RequestsRetryCommand.is_success(response):
            return_value = response.result["versions"]
        else:
            print("Failed to acquire versions - {}".format(file_identifier))
            logger.warn("Failed to acquire versions : C:{} E:{}".format(response.status_code, response.error))

        return StorageFileVersionResponse(return_value, response)

