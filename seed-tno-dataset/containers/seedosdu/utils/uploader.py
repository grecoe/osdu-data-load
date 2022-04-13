import multiprocessing
import typing
import math
import os
from utils.logutil import LogBase, Logger
from utils.configuration.config import Config
from utils.requests.auth import Credential
from utils.requests.file import FileRequests
from utils.requests.metagenerator import MetadataGenerator
from utils.requests.storage import StorageRequests
from joblib import Parallel, delayed

class FileUploadResult:
    def __init__(self):
        self.succeeded:bool = False
        self.file_name = None
        self.file_id:str = None
        self.file_source:str = None
        self.file_version:str = None

class UploadResults:
    def __init__(self, upload_results:typing.List[FileUploadResult]):
        self.success:typing.List[FileUploadResult] = [x for x in upload_results if x.succeeded]
        self.failed:typing.List[FileUploadResult] = [x for x in upload_results if not x.succeeded]

class FileUploader(LogBase):
    def __init__(self, file_list:typing.List[str], config:Config, credentials:Credential):
        super().__init__("Uploader", config.file_share_mount, config.log_identity)
        self.file_list:typing.List[str] = file_list
        self.config:Config = config
        self.credentials:Credential = credentials

    def upload_files(self) -> UploadResults:

        n_cores = multiprocessing.cpu_count()
        n_jobs = self.config.batch_multiplier * n_cores

        logger:Logger = self.get_logger()

        logger.info(f"Available Cores: {n_cores}")
        logger.info(f"Batch Multiplier: {self.config.batch_multiplier}")
        logger.info(f"Batch Size: {n_jobs}")
        logger.info(f"File Count: {len(self.file_list)}")
        print(f"Upload File Count: {len(self.file_list)}")

        # Utilities for processing
        file_requests = FileRequests(self.config, self.credentials.get_application_token())
        storage_requests = StorageRequests(self.config, self.credentials.get_application_token())

        # Collection of the results
        batch_results:typing.List[FileUploadResult] = []

        # Tracking information
        current_batch = 0
        max_batch = math.ceil(len(self.file_list)/n_jobs)

        # Process batches
        for file_batch in self._batch(self.file_list, n_jobs):
            current_batch += 1
            batch_message = f"Uploading batch - {current_batch} of {max_batch}" 
            print(batch_message)
            logger.info(batch_message)

            batch_results += Parallel(n_jobs=n_jobs)(delayed(self._upload_single_file)(file, file_requests, storage_requests) for file in file_batch)

        # Report on results
        return_results:UploadResults = UploadResults(batch_results)
        logger.info(f"Files Processed: {len(batch_results)}")
        logger.info(f"Succesful Uploads: {len(return_results.success)}")
        logger.info(f"Failed Uploads: {len(return_results.failed)}")

        if len(return_results.failed):
            logger.info("Files not uploaded:")
            for result in return_results.failed:
                logger.info(result.file_name)


        return return_results


    def _upload_single_file(self, file_name:str, file_requests:FileRequests, storage_requests:StorageRequests) -> FileUploadResult:

        logger:Logger = self.get_logger()

        return_result = FileUploadResult()
        return_result.file_name = os.path.split(file_name)[-1]

        upload = file_requests.get_upload_url()
        if upload:
            return_result.file_source = upload.FileSource

            if file_requests.upload_file(upload, file_name):
                metadata = MetadataGenerator.generate_metadata(self.config, upload, file_name)
                return_result.file_id = file_requests.upload_metadata(metadata)
                if return_result.file_id:
                    versions = storage_requests.get_file_versions(return_result.file_id)
                    if versions:
                        return_result.file_version = versions[0]
                        return_result.succeeded = True
                    else:
                        logger.error(f"Failed to get file versions for {file_name}")
                else:
                        logger.error(f"Failed to get file ID on metadata for {file_name}")
            else:
                logger.error(f"Failed to upload file {file_name}")

        else:
            logger.error("Failed to acquire upload url")

        return return_result

    def _batch(self, items:list, batch_size:int) -> typing.List[str]:
        idx = 0
        while idx < len(items):
            yield items[idx: idx + batch_size]
            idx += batch_size
