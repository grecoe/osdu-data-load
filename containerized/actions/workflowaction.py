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
from utils.generator.metadatagenerator import MetadataGenerator
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
        file_requests = None
        storage_requests = None
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
                batch_results += Parallel(n_jobs=n_jobs, timeout=600.0)(delayed(self._process_single_record)(record, file_requests, storage_requests) for record in record_batch)
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


    def _process_single_record(self, record:Record, file_requests, storage_requests) -> RecordUploadResult:
        
        logger:Logger = self.get_logger()

        # TODO - Actual upload to OAK
        return_result:RecordUploadResult = RecordUploadResult()
        return_result.file_name = record.file_name
        return_result.record_identity = record.RowKey
        return_result.succeeded = True

        return return_result

    def _batch(self, items:list, batch_size:int) -> typing.List[str]:
        """list is generic because it uses different types"""
        idx = 0
        while idx < len(items):
            yield items[idx: idx + batch_size]
            idx += batch_size
