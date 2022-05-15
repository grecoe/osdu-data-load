import os
import json
import typing
import math
import multiprocessing
from datetime import datetime, timedelta
from utils.configuration.configutil import Config
from utils.storage.storagetable import AzureTableStoreUtil
from utils.storage.record import Record
from utils.storage.share import FileShareUtil
from utils.requests.auth import Credential
from utils.requests.retryrequest import RetryRequestResponse
from utils.requests.fileservice import FileRequests, FileUploadUrlResponse, FileUploadMetadataResponse
from utils.requests.storageservice import StorageRequests, StorageFileVersionResponse

from utils.log.logutil import LogBase, Logger
from joblib import Parallel, delayed

class RecordUploadResult:
    def __init__(self):
        # Flag indicating succesful processing
        self.succeeded:bool = False
        # Record ID in storage table
        self.record_identity:str = None
        # File name to process
        self.file_name = None
        # Metadata id in OSDU
        self.file_id:str = None
        # File source from UploadURL
        self.file_source:str = None
        # File version after upload to OSDU
        self.file_version:str = None
        # Status code, will use this if it failse
        self.status_code = None
        # Series of status codes during upload
        self.status_codes = {}
        # Series of connection errors during upload
        self.connection_errors = {}
        # Total number of attempts in talking with OSDU
        self.total_attempts = 0

    def update_status(self, response:RetryRequestResponse):
        
        self.total_attempts += response.attempts
        self.status_code = response.status_code

        if response.status_codes:
            for code in response.status_codes:
                if code not in self.status_codes:
                    self.status_codes[code] = 0
                self.status_codes[code] += 1

        if len(response.connection_errors):
            for val in response.connection_errors:
                if val not in self.connection_errors:
                    self.connection_errors[val] = 0
                self.connection_errors[val] += 1

class WorkflowAction(LogBase):
    def __init__(self, configuration:Config):
        super().__init__("Workflow", configuration.mounted_file_share_name, configuration.log_identity, True)
        self.configuration = configuration

    def process_records(self):
        """
        Processes a group of records that have been fed into the process. The records come in a form
        of record id in the storage table. 

        """
        logger:Logger = self.get_logger()

        ######################################################################
        # Figure out batch sizing and dump out basics 
        n_cores = multiprocessing.cpu_count()
        n_jobs = int(self.configuration.batch_multiplier) * n_cores

        logger.info("Process - {}".format(self.configuration.log_identity))
        logger.info("Workflow - {}".format(self.configuration.workflow_record))
        logger.info(f"Available Cores: {n_cores}")
        logger.info(f"Batch Multiplier: {self.configuration.batch_multiplier}")
        logger.info(f"Batch Size: {n_jobs}")

        ######################################################################
        # Storage table to collect and update records on files
        table_util = AzureTableStoreUtil(
            self.configuration.record_account, 
            self.configuration.record_account_key)

        ######################################################################
        # Load the file with the record ID's in the table.File is contained in the file 
        # share associated with the container. This is mimicked locally by just setting
        # file share to "./"
        workflow_items:typing.List[str] = []
        with open(self.configuration.workflow_record, "r") as workflow_tasks:
            file_data = workflow_tasks.readlines()
            file_data = "\n".join(file_data)
            workflow_items = json.loads(file_data)

        print("Workflow {} process {} records".format(self.configuration.workflow_record, len(workflow_items)))
        logger.info("{} processing {} records".format(self.configuration.workflow_record, len(workflow_items)))

        ######################################################################
        # FOr each id in the manifest, try and find the record in the storage table
        record_list:typing.List[Record] = []
        for record_id_batch in self._batch(workflow_items, n_jobs):
            try:
                record_list += Parallel(n_jobs=n_jobs, timeout=600.0)(delayed(self._search_single_record)(record_id, table_util) for record_id in record_id_batch)
                record_list = [x for x in record_list if x is not None]
            except Exception as ex:
                logger.info("Generic Exception - Table Search")
                logger.info(str(ex))

        ######################################################################
        # With the list of records retrieved from the storage table, ensure we
        # actually have some work to perform. If not just report it and return.
        print("Retrieved {} records from the table".format(len(record_list)))
        logger.info("Retrieved {} records from the table".format(len(record_list)))
        if len(record_list) == 0:
            logger.info("There are no files to process at this time.")
            return

        ######################################################################
        # Prepare the services we'll need for processing
        current_batch = 0
        max_batch = math.ceil(len(record_list)/n_jobs)

        client_credentials = Credential(self.configuration)
        file_requests = FileRequests(self.configuration, client_credentials.get_application_token())
        storage_requests = StorageRequests(self.configuration, client_credentials.get_application_token())
        metadata_storage = FileShareUtil(
            self.configuration.record_account,
            self.configuration.record_account_key,
            self.configuration.record_account_share
        )
        

        ######################################################################
        # Batch process each record into OSDU 
        batch_results:typing.List[RecordUploadResult] = []

        for record_batch in self._batch(record_list, n_jobs):
            current_batch += 1
            batch_message = f"Uploading batch - {current_batch} of {max_batch}" 
            print(batch_message)
            logger.info(batch_message)

            try:
                batch_results += Parallel(n_jobs=n_jobs, timeout=600.0)(delayed(self._process_single_record)(record, metadata_storage, file_requests, storage_requests) for record in record_batch)
            except Exception as ex:
                logger.info("Generic Exception - Processing")
                logger.info(str(ex))

        ######################################################################
        # Batch process each completed records that succeeded back to the 
        # storage table for auditing purposes. 
        logger.info("Update records in storage table with {} results".format(len(batch_results)))
        for execution_results in self._batch(batch_results, n_jobs):
            try:
                Parallel(n_jobs=n_jobs, timeout=600.0)(delayed(self._finalize_single_record)(execution_result, record_list, table_util) for execution_result in execution_results)
            except Exception as ex:
                logger.info("Generic Exception - Record Update")
                logger.info(str(ex))


        # Dump out some info on how many were succesfully processed
        good = [x for x in batch_results if x.succeeded]
        print("{} records succesfully processed".format(len(good)))
        logger.info("{} records succesfully processed".format(len(good)))

    def _finalize_single_record(
        self, 
        execution_result:RecordUploadResult, 
        record_list:typing.List[Record], 
        table_util:AzureTableStoreUtil
        ) -> None:
        """
        Batch processor for records that have been through the flow. If a record is
        tagged as haviing successfully processed, it is in OSDU and we can update the
        storage table to reflect who did it, at what time, and the metadata id in OSDU
        of the record that was pushed.

        Parameters:

        execution_result: 
            The object used to track processing information.
        record_list: 
            Records representing records in the storage table that we started with
        table_util: 
            Utility to talk with the storage table. 

        Returns 
            None
        """
        logger:Logger = self.get_logger()
        
        find_record = [x for x in record_list if x.RowKey == execution_result.record_identity]
        if len(find_record):
            orig_record = find_record[0]
            orig_record.processed_time = str(datetime.utcnow())
            if execution_result.succeeded:
                orig_record.container_id = self.configuration.log_identity
                orig_record.processed = True
                orig_record.meta_id = execution_result.file_id
            else:
                orig_record.code = execution_result.status_code
                logger.warn("Record {} failed to process".format(execution_result.record_identity))

            table_util.update_record(self.configuration.record_storage_table, orig_record)


    def _search_single_record(self, record_id:str, table_util:AzureTableStoreUtil) -> Record:
        """
        Batch processor for searching for a record in table storage.

        Parameters:

        record_id: 
            Record id found in the workflow manifest
        table_util: 
            Utility to talk with the storage table. 

        Returns 
            Record if found, None otherwise
        """
        return_item:Record = None
        records = table_util.search_table_id(self.configuration.record_storage_table, record_id)
        if records:
            if len(records):
                return_item = records[0]
        return return_item


    def _process_single_record(
        self, 
        record:Record,
        metadata_storage:FileShareUtil, 
        file_requests:FileRequests, 
        storage_requests:StorageRequests) -> RecordUploadResult:
        """
        Processes a single record into OSDU with all of the stages required
        - Download metadata from record store
        - Get an upload URL
        - Update the meta with fileSource
        - Upload record
        - Upload metadata
        - Get file version

        File only succesful if all above steps succeed. 

        Transfer file is a BIG issue because we don't have rights to the SAS given by 
        OSDU so we might need to tweak random sleep times based on file size?

        Parameters:

        record: 
            The table storage record we are to work on.
        metadata_storage:
            Storage where the metadata record can be found
        file_requests:
            Utility for talking OSDU file service
        storage_requets:
            Utiltity for talking OSDU storage service

        Returns:
            RecordUploadResult

        Notes:
        We do not have rights to the SAS token recieved for the OSDU upload location so 
        we cannot query for properties on the copy operation. Given that, we are using a 
        strategy where we assume we have ~2MBS throughput and "guess" at the wait time. 
        """
        
        logger:Logger = self.get_logger()

        # Prepare a return result
        return_result:RecordUploadResult = RecordUploadResult()
        return_result.file_name = record.file_name
        return_result.record_identity = record.RowKey
        return_result.succeeded = False

        ################################################################
        # Stored metadata location -> records/FI.json
        # Get the metadata file 
        stored_metadata = os.path.split(record.metadata)
        folder = stored_metadata[0]
        file = stored_metadata[1]
        local_folder = "./"
        local_file = os.path.join(local_folder, file)

        metadata_storage.download_file(local_folder, folder, file )
        if os.path.exists(local_file):
            # We have issues if the metadata is not there.
            raw_meta = None
            with open(local_file, "r") as meta_file:
                raw_meta = meta_file.readlines()
                raw_meta = "\n".join(raw_meta)
            
            os.remove(local_file)

            if "||UPLOAD_URL||" not in raw_meta:
                logger.error("Invalid Metadata recieved for : {}".format(record.metadata))
                return return_result

        ################################################################
        # Get upload URL and then force it in the metadata
        upload_response:FileUploadUrlResponse = file_requests.get_upload_url()
        return_result.update_status(upload_response.response)

        if upload_response.url:
            return_result.file_source = upload_response.url.FileSource
            raw_meta = raw_meta.replace("||UPLOAD_URL||", upload_response.url.FileSource)
            functional_meta = json.loads(raw_meta)

            ################################################################
            # Upload the file from customer storage to OSDU
            file_size_mb = int(record.file_size) / (1024 * 1024)
            file_size_mb = int(math.floor(file_size_mb) / 2)

            if file_requests.transfer_file(file_size_mb, upload_response.url, record.source_sas):
                ################################################################
                # Upload the metadata to OSDU
                upload_meta_response:FileUploadMetadataResponse = file_requests.upload_metadata(functional_meta)
                return_result.update_status(upload_meta_response.response)
                
                return_result.file_id = upload_meta_response.id

                if return_result.file_id:
                    ################################################################
                    # Get file version to verify it made it
                    versions_response:StorageFileVersionResponse = storage_requests.get_file_versions(return_result.file_id)
                    return_result.update_status(versions_response.response)
                
                    if versions_response.versions:
                        return_result.file_version = versions_response.versions[0]
                        return_result.succeeded = True
                    else:
                        logger.error(f"Failed to get file versions for {record.file_name}")
                else:
                        logger.error(f"Failed to get file ID on metadata for {record.file_name}")
        
            else:
                logger.error("File {} failed to upload".format(record.file_name))

        else:
            logger.error("Failed to get upload url for {}".format(record.file_name))

        return return_result

    def _batch(self, items:list, batch_size:int) -> typing.List[str]:
        """list is generic because it uses different types, batches up 
        a list based on size of batch requested and returns a sub list
        with that many items in it until it is exhausted"""
        idx = 0
        while idx < len(items):
            yield items[idx: idx + batch_size]
            idx += batch_size
