##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import os
import json
import math
import multiprocessing
import typing
import time
from utils.configuration.configutil import Config
from utils.storage.storagetable import AzureTableStoreUtil
from utils.storage.record import Record
from utils.storage.share import FileDetails, FileShareUtil
from utils.generator.metadatagenerator import MetadataGenerator
from utils.log.logutil import LogBase, Logger
from joblib import Parallel, delayed


class ScanAction(LogBase):
    def __init__(self, configuration:Config):
        super().__init__("ScanAction", configuration.mounted_file_share_name, configuration.log_identity)
        self.configuration = configuration

    def scan_customer_storage(self):
        """
        Scans a storage account identified in the configuration with the data stored in the
        configuration settings (representing an Azure File Share)
        
        self.configuration.source_account  
        self.configuration.source_account_key 
        self.configuration.source_account_share

        And filters items based on 
        self.configuration.data_source_map

        It creates two items per file found
            - Record in an Azure Storage table tracking the file
            - Metadata record in Azure Storage File share for later use in uploading
        """

        # Get our logger
        logger:Logger = self.get_logger()

        ######################################################################
        # Figure out batch sizing 
        n_cores = multiprocessing.cpu_count()
        n_jobs = int(self.configuration.batch_multiplier) * n_cores

        ######################################################################
        # Storage Shares, one for input (source) one for output (record)
        logger.info("Customer Storage Account: {}".format(self.configuration.source_account))
        logger.info("Customer File Share: {}".format(self.configuration.source_account_share))
        source_share_util = FileShareUtil(
            self.configuration.source_account,  
            self.configuration.source_account_key, 
            self.configuration.source_account_share)

        logger.info("Record Storage Account: {}".format(self.configuration.record_account))
        logger.info("Record File Share: {}".format(self.configuration.record_account_share))
        record_share_util = FileShareUtil(
            self.configuration.record_account, 
            self.configuration.record_account_key, 
            self.configuration.record_account_share)

        ######################################################################
        # Storage table to track files
        table_util:AzureTableStoreUtil = AzureTableStoreUtil(
            self.configuration.record_account, 
            self.configuration.record_account_key)

        # Make sure output folders exist for metadata generation
        record_share_util.create_directory(self.configuration.record_metadata_path)


        ######################################################################
        # Filter messages from the storage based on the data_source_map
        logger.info("Source Map:")
        logger.info(self.configuration.data_source_map)
        for path in self.configuration.data_source_map: 

            # If the path is created, then it clearly has no files and should be skipped
            created_directory = source_share_util.create_directory(path)
            logger.info("Requested Path Exists ; {}: {}".format(path, not created_directory))

            if created_directory:
                print("Path does not exist in the file share, skipping")
                continue

            # Collect a list of files from the source folder
            files = source_share_util.list_files(path)
            files = [x for x in files if x.file_name.lower().split(".")[-1] in self.configuration.data_source_map[path]]

            # Tracking information
            current_batch = 0
            max_batch = math.ceil(len(files)/n_jobs)
            process_results:typing.List[str] = []
            
            ######################################################################
            # For each file path (directory) process the files. 
            logger.info("Files to process in path : {}: {}".format(path, len(files)))
            for file_batch in self._batch(files, n_jobs):

                current_batch += 1
                batch_message = f"Processing batch of found records - {current_batch} of {max_batch}" 
                print(batch_message)
                logger.info(batch_message)

                try:
                    process_results += Parallel(n_jobs=n_jobs, timeout=600.0)(delayed(self._process_file)(path, record, record_share_util, table_util) for record in file_batch)
                except Exception as ex:
                    logger.info("Generic Exception")
                    logger.info(str(ex))

                time.sleep(2)

            processed = process_results.count("1")
            duplicate = process_results.count("0") 

            message = "{} : {} registered, {} duplicate".format(
                path,
                processed,
                duplicate
            )

            logger.info(message)
            print(message)


    def _process_file(
        self, 
        path:str,
        source_file:FileDetails,
        record_share_util:FileShareUtil,
        table_util:AzureTableStoreUtil 
        ) -> str:
        """
        Batch process for each file. Checks to see if the record has already been recorded
        in the Azure Storage table. If so, it is ignored, if not:

        - Generate the metadata to associate with the original file, and upload it to the 
          record file share 
        - Create an Azure Table Storage entry with information about the file. 

        Parameters:

        path:
            The path in the external share for this file. 
        source_file:
            Details about the source file from the external file share, including the SAS URL 
        record_share_util:
            Azure Storage File Share to store metadata in
        table_util:
            Azure Storage Table to record the file
        """
        return_value = "0"

        file_name = "{}/{}".format(path, source_file.file_name)
        file_name_base = source_file.file_name.split(".")[0]

        # If the file exists in the table, then this is likely a re-run and we should skip. 
        # Customer work around is to delete the records in the storage table. 
        exists = table_util.search_table_filename(self.configuration.record_storage_table, file_name)
        if len(exists) == 0:
            return_value = "1"
            # Create metadata file and upload it to the record share file share
            metadata = MetadataGenerator.generate_metadata(
                self.configuration.acl_viewer, 
                self.configuration.acl_owner, 
                self.configuration.legal_tag, 
                file_name)

            # TODO : Change the name base here to a UUID so there is no name conflicts.
            metadata_file = "{}.json".format(file_name_base) 
            with open(metadata_file, "w") as meta_output:
                        meta_output.writelines(json.dumps(metadata, indent=4))
            record_share_util.upload_file(self.configuration.record_metadata_path, metadata_file)
            os.remove(metadata_file)

            # Add an entry to the storage table for this file. 
            r = Record(self.configuration.record_storage_partition)
            r.file_name = file_name
            r.file_size = source_file.file_size
            r.source_sas = source_file.file_url
            r.metadata = "{}/{}".format(self.configuration.record_metadata_path, metadata_file)
        
            table_util.add_record(self.configuration.record_storage_table, r)        

        return return_value

    def _batch(self, items:typing.List[FileDetails], batch_size:int) -> typing.List[str]:
        """Batch a list of file detailsbased on size of batch requested and returns a sub list
        with that many items in it until it is exhausted"""
        idx = 0
        while idx < len(items):
            yield items[idx: idx + batch_size]
            idx += batch_size
