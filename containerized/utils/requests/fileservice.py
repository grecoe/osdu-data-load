##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import os
import requests
import time
from utils.log.logutil import LogBase, Logger
from utils.configuration.configutil import Config
from utils.requests.retryrequest import RequestsRetryCommand, RetryRequestResponse
from azure.storage.blob import BlobClient

class UploadUrl:
    """
    Encapsulates the return of the Upload URL recieved
    from an OSDU instance
    """
    def __init__(self, upload_response:dict):
        self.SignedURL = None
        self.FileSource = None

        for key in upload_response:
            setattr(self, key, upload_response[key])

class FileUploadUrlResponse:
    """
    Encapsulates the response from retrieving an upload url from an OSDU
    instances along with the request statistics. 
    """
    def __init__(self, url:UploadUrl, response:RetryRequestResponse):
        self.url:UploadUrl = url
        self.response:RetryRequestResponse = response

class FileUploadMetadataResponse:
    """
    Encapsulates the response from uploading metadata to an OSDU
    instances along with the request statistics. 
    """
    def __init__(self, id:str, response:RetryRequestResponse):
        self.id:str = id
        self.response:RetryRequestResponse = response

class FileRequests(LogBase):
    """
    Encapsulate calls to the OSDU File Service
    """
    def __init__(self, configuration:Config, access_token:str):
        super().__init__("FileRequests", configuration.mounted_file_share_name, configuration.log_identity, True)
        self.configuration:Config = configuration
        self.token:str = access_token

    def get_upload_url(self) -> FileUploadUrlResponse:
        """
        Retrieve an upload URL from OSDU
        """

        logger:Logger = self.get_logger()

        url = self.configuration.file_url + "/files/uploadURL"
        headers = self.configuration.get_headers(self.token)

        response:RetryRequestResponse = RequestsRetryCommand.make_request(
            requests.get,
            url,
            headers=headers
        )

        if response.attempts > 1:
            message = f"get_upload_url - {response.action} on {response.url} attempts : {response.attempts} codes : {response.status_codes}"
            message_related = f"get_upload_url : Correlation - {response.status_error_map}"
            print(message_related)
            logger.info(message_related)
            print(message)
            logger.info(message)

        return_value = None
        if RequestsRetryCommand.is_success(response):
            return_value = UploadUrl(response.result["Location"])
        else:
            print("Failed to get upload url - {}".format(response.status_code))
            logger.warn("Failed to get upload url : C:{} E:{}".format(response.status_code, response.error))

        return FileUploadUrlResponse(return_value, response)

    def transfer_file(self, file_size_mb:int, url:UploadUrl, sas_url:str) -> bool:
        """
        Transfer a file from one Azure Storage Account location (url.SignedUrl - OSDU) to 
        another location (sas_url)

        The SAS Url from OSDU does not allow us to query the blob properties, so we have to 
        make an assumption that we can transfer an MB in 1 second. In reality, the caller is
        assuming 2MBS. Best shot, but probably should review. 

        Parameters:
        file_size_mb:
            Size of the file to transfer in MB, used to determine sleep 
        url: 
            retrieved from getUploadUrl
        sas_url: 
            the blob to move

        Returns:
            True
        """

        # Get blob client on target
        target_blob = BlobClient.from_blob_url(url.SignedURL)

        # Copy source to target
        target_blob.start_copy_from_url(sas_url)

        # TODO Just try and sleep, we have no right on the blob to get it's properties
        # so this is an issue.....we'll have to wait some amount of time to see
        # if size becomes an issue
        sleep_time = file_size_mb * 1
        time.sleep(sleep_time)

        return True

    def upload_file(self, url:UploadUrl, file_path:str) -> bool:
        """
        Upload a local file with a signed OSDU URL from a local file. 

        Parameters:
        url: 
            retrieved from getUploadUrl
        file_path: 
            Local file path

        Returns:
            True if succesful, False otherwise.
        """
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
        
    def upload_metadata(self, metadata:dict) -> FileUploadMetadataResponse:
        """
        Upload a metadata file to OSDU

        Parameters:
        metadata:
            Dictionary containing the metadata associated with an uploaded file. 
        """
        logger:Logger = self.get_logger()

        url = self.configuration.file_url + "/files/metadata"
        headers = self.configuration.get_headers(self.token)

        response:RetryRequestResponse = RequestsRetryCommand.make_request(
            requests.post,
            url,
            headers=headers,
            json=metadata
        )

        if response.attempts > 1:
            message = f"upload_metadata - {response.action} on {response.url} attempts : {response.attempts} codes : {response.status_codes}"
            message_related = f"upload_metadata : Correlation - {response.status_error_map}"
            print(message_related)
            logger.info(message_related)
            print(message)
            logger.info(message)

        return_value = None
        if RequestsRetryCommand.is_success(response) and response.status_code == 201:
            return_value = response.result["id"]
        else:
            print("Failed to upload metadata - {}".format(response.status_code))
            logger.warn("Failed to upload metadata : C:{} E:{}".format(response.status_code, response.error))

        return FileUploadMetadataResponse(return_value, response)

