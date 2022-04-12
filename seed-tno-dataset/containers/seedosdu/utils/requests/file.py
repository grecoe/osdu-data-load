import os
import requests
from utils.logutil import LogBase, Logger
from utils.configuration.config import Config
from utils.requests.retryrequests import RequestsRetryCommand, RetryRequestResponse
from azure.storage.blob import BlobClient

class UploadUrl:
    def __init__(self, upload_response:dict):
        self.SignedURL = None
        self.FileSource = None

        for key in upload_response:
            setattr(self, key, upload_response[key])

class FileRequests(LogBase):
    def __init__(self, configuration:Config, access_token:str):
        super().__init__("FileRequests", configuration.file_share_mount)
        self.configuration:Config = configuration
        self.token:str = access_token

    def get_upload_url(self) -> UploadUrl:

        logger:Logger = self.get_logger()

        url = self.configuration.FileURL + "/files/uploadURL"
        headers = self.configuration.get_headers(self.token)

        response:RetryRequestResponse = RequestsRetryCommand.make_request(
            requests.get,
            url,
            headers=headers
        )

        if response.attempts > 1:
            #TODO 
            message = f"get_upload_url - {response.action} on {response.url} attempts : {response.attempts}"
            print(message)
            logger.info(message)

        return_value = None
        if RequestsRetryCommand.is_success(response):
            return_value = UploadUrl(response.result["Location"])
        else:
            print("Failed to get upload url - {}".format(response.status_code))
            logger.warn("Failed to get upload url : C:{} E:{}".format(response.status_code, response.error))

        return return_value

    def upload_file(self, url:UploadUrl, file_path:str) -> bool:
        logger:Logger = self.get_logger()

        upload_success = False

        if not os.path.exists(file_path):
            logger.error(f"File {file_path} does not exist")
            raise Exception("File {} does not exist".format(file_path))

        blob_client = BlobClient.from_blob_url(url.SignedURL)
        with open(file_path, "rb") as las_file:
            upload_response = blob_client.upload_blob(
                las_file, blob_type="BlockBlob", overwrite=True)

            logger.debug(f"Blob upload response {upload_response}")
            upload_success = True

        return upload_success
        
    def upload_metadata(self, metadata:dict) -> str:

        logger:Logger = self.get_logger()

        url = self.configuration.FileURL + "/files/metadata"
        headers = self.configuration.get_headers(self.token)

        response:RetryRequestResponse = RequestsRetryCommand.make_request(
            requests.post,
            url,
            headers=headers,
            json=metadata
        )

        if response.attempts > 1:
            message = f"upload_metadata - {response.action} on {response.url} attempts : {response.attempts}"
            print(message)
            logger.info(message)

        return_value = None
        if RequestsRetryCommand.is_success(response) and response.status_code == 201:
            return_value = response.result["id"]
        else:
            print("Failed to upload metadata - {}".format(response.status_code))
            logger.warn("Failed to upload metadata : C:{} E:{}".format(response.status_code, response.error))

        return return_value

