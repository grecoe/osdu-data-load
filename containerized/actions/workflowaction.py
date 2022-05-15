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
from utils.requests.fileservice import FileRequests, FileUploadUrlResponse, FileUploadMetadataResponse
from utils.requests.storageservice import StorageRequests, StorageFileVersionResponse

from utils.log.logutil import LogBase, Logger
from joblib import Parallel, delayed

class RecordUploadResult:
    def __init__(self):
        self.succeeded:bool = False
        self.record_identity:str = None
        self.file_name = None

        # TODO on the following when we bring over the requests
        self.file_id:str = None
        self.file_source:str = None
        self.file_version:str = None
        self.status_codes = {}
        self.connection_errors = {}


class WorkflowAction(LogBase):
    def __init__(self, configuration:Config):
        super().__init__("Workflow", configuration.mounted_file_share_name, configuration.log_identity, True)
        self.configuration = configuration

    def process_records(self):

        logger:Logger = self.get_logger()

        # Figure out batch sizing 
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

        # Load the file with the record ID's in the table.
        workflow_items:typing.List[str] = []
        with open(self.configuration.workflow_record, "r") as workflow_tasks:
            file_data = workflow_tasks.readlines()
            file_data = "\n".join(file_data)
            workflow_items = json.loads(file_data)

        print("Workflow {} process {} records".format(self.configuration.workflow_record, len(workflow_items)))
        logger.info("{} processing {} records".format(self.configuration.workflow_record, len(workflow_items)))

        # Get instances of Record from the table
        # TODO - REMOVE
        start = datetime.utcnow()
        # TODO - REMOVE

        record_list:typing.List[Record] = []
        
        for record_id_batch in self._batch(workflow_items, n_jobs):
            try:
                record_list += Parallel(n_jobs=n_jobs, timeout=600.0)(delayed(self._search_single_record)(record_id, table_util) for record_id in record_id_batch)
                record_list = [x for x in record_list if x is not None]
            except Exception as ex:
                logger.info("Generic Exception - Table Search")
                logger.info(str(ex))

        # TODO - REMOVE
        end = datetime.utcnow()
        logger.info("***TABLE RETRIEVE : {}".format((end-start).total_seconds()))
        # TODO - REMOVE

        print("Retrieved {} records from the table".format(len(record_list)))
        logger.info("Retrieved {} records from the table".format(len(record_list)))

        if len(record_list) == 0:
            logger.info("There are no files to process at this time.")
            return

        # Tracking information
        current_batch = 0
        max_batch = math.ceil(len(record_list)/n_jobs)

        # TODO: File and Storage Services
        # Utilities for processing
        client_credentials = Credential(self.configuration)
        file_requests = FileRequests(self.configuration, client_credentials.get_application_token())
        storage_requests = StorageRequests(self.configuration, client_credentials.get_application_token())
        metadata_storage = FileShareUtil(
            self.configuration.record_account,
            self.configuration.record_account_key,
            self.configuration.record_account_share
        )
        
        batch_results:typing.List[RecordUploadResult] = []

        # Process batches
        # TODO - REMOVE
        start = datetime.utcnow()
        # TODO - REMOVE
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

        # TODO - REMOVE
        end = datetime.utcnow()
        logger.info("***RECORD PROCESSING : {}".format((end-start).total_seconds()))
        # TODO - REMOVE


        # TODO - REMOVE
        start = datetime.utcnow()
        # TODO - REMOVE

        logger.info("Marking succesful records in storage table, check {} results".format(len(batch_results)))
        for execution_results in self._batch(batch_results, n_jobs):
            try:
                Parallel(n_jobs=n_jobs, timeout=600.0)(delayed(self._finalize_single_record)(execution_result, record_list, table_util) for execution_result in execution_results)
            except Exception as ex:
                logger.info("Generic Exception - Record Update")
                logger.info(str(ex))


        good = [x for x in batch_results if x.succeeded]
        print("Resulted in {} GOOD results".format(len(good)))

        # TODO - REMOVE
        end = datetime.utcnow()
        logger.info("***RECORD UPDATING : {}".format((end-start).total_seconds()))
        # TODO - REMOVE

    def _finalize_single_record(
        self, 
        execution_result:RecordUploadResult, 
        record_list:typing.List[Record], 
        table_util:AzureTableStoreUtil
        ) -> None:
    
        logger:Logger = self.get_logger()
        
        if execution_result.succeeded:
            find_record = [x for x in record_list if x.RowKey == execution_result.record_identity]
            if len(find_record):
                success_record = find_record[0]
                success_record.container_id = self.configuration.log_identity
                success_record.processed = True
                success_record.meta_id = execution_result.file_id
                success_record.processed_time = str(datetime.utcnow())

                table_util.update_record(self.configuration.record_storage_table, success_record)
        else:
            logger.warn("Record {} failed to process".format(execution_result.record_identity))


    def _search_single_record(self, record_id:str, table_util:AzureTableStoreUtil) -> Record:
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

        Timing for a single record shows the issue:

            ***DOWNLOAD META : 0.628114
            ***GET UPLOAD URL : 0.90549
            *** TRANSFER FILE : 10.652775
            *** UPLOAD METADATA : 2.250204
            *** GET VERSION : 0.529724        
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

        # TODO - REMOVE
        start = datetime.utcnow()
        # TODO - REMOVE
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
        # TODO - REMOVE
        end = datetime.utcnow()
        logger.info("***DOWNLOAD META : {}".format((end-start).total_seconds()))
        # TODO - REMOVE

        ################################################################
        # Get upload URL and then force it in the metadata
        # TODO - REMOVE
        start = datetime.utcnow()
        # TODO - REMOVE
        upload_response:FileUploadUrlResponse = file_requests.get_upload_url()
        # TODO - REMOVE
        end = datetime.utcnow()
        logger.info("***GET UPLOAD URL : {}".format((end-start).total_seconds()))
        # TODO - REMOVE

        if upload_response.response.attempts > 1:
            print("TODO Get UploadUrl attempts : {}".format(upload_response.response.attempts))

        if upload_response.url:
            return_result.file_source = upload_response.url.FileSource
            raw_meta = raw_meta.replace("||UPLOAD_URL||", upload_response.url.FileSource)
            functional_meta = json.loads(raw_meta)

            ################################################################
            # Upload the file from customer storage to OSDU
            # TODO - REMOVE
            start = datetime.utcnow()
            # TODO - REMOVE

            # TODO Test
            file_size_mb = int(record.file_size) / (1024 * 1024)
            file_size_mb = int(math.floor(file_size_mb) / 2)

            if file_requests.transfer_file(file_size_mb, upload_response.url, record.source_sas):
                logger.info("File succesfully uploaded")

                # TODO - REMOVE
                end = datetime.utcnow()
                logger.info("*** TRANSFER FILE : {}".format((end-start).total_seconds()))
                # TODO - REMOVE

                ################################################################
                # Upload the metadata to OSDU
                # TODO - REMOVE
                start = datetime.utcnow()
                # TODO - REMOVE
                upload_meta_response:FileUploadMetadataResponse = file_requests.upload_metadata(functional_meta)
                # TODO  ? return_result.updateStatus(upload_meta_response.response)
                if upload_meta_response.response.attempts > 1:
                    print("TODO Upload metadata attempts : {}".format(upload_meta_response.response.attempts))
                # TODO - REMOVE
                end = datetime.utcnow()
                logger.info("*** UPLOAD METADATA : {}".format((end-start).total_seconds()))
                # TODO - REMOVE

                return_result.file_id = upload_meta_response.id

                if return_result.file_id:
                    ################################################################
                    # Get file version to verify it made it
                    # TODO - REMOVE
                    start = datetime.utcnow()
                    # TODO - REMOVE
                    versions_response:StorageFileVersionResponse = storage_requests.get_file_versions(return_result.file_id)
                    # TODO - REMOVE
                    end = datetime.utcnow()
                    logger.info("*** GET VERSION : {}".format((end-start).total_seconds()))
                    # TODO - REMOVE

                    # TODO ? return_result.updateStatus(versions_response.response)
                    if versions_response.response.attempts > 1:
                        print("TODO Get Version attempts : {}".format(versions_response.response.attempts))
                
                    if versions_response.versions:
                        return_result.file_version = versions_response.versions[0]
                        return_result.succeeded = True
                    else:
                        print("TODO: GET VERSION FAILED")
                        logger.error(f"Failed to get file versions for {record.file_name}")
                else:
                        print("TODO: GET ID FAILED")
                        logger.error(f"Failed to get file ID on metadata for {record.file_name}")
        
            else:
                print("TODO: UPLOAD FAILED")
                logger.error("File {} failed to upload".format(record.file_name))

        else:
            print("TODO: NO UPLOAD URL")
            logger.error("Failed to get upload url for {}".format(record.file_name))

            # TODO - Complete the process to upload file then metadata and collect 
            #        other information.
            #return_result.status_codes = []
            #return_result.file_version = None
            #return_result.connection_errors = None
            #return_result.file_id = None

        return return_result

    def _batch(self, items:list, batch_size:int) -> typing.List[str]:
        """list is generic because it uses different types"""
        idx = 0
        while idx < len(items):
            yield items[idx: idx + batch_size]
            idx += batch_size
