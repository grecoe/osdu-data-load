##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import os
import json
import typing
from utils.configuration.configutil import Config
from utils.storage.storagetable import AzureTableStoreUtil
from utils.storage.record import Record
from utils.storage.share import FileShareUtil
from utils.log.logutil import LogBase, Logger

class RoundRobin(LogBase):
    def __init__(self, configuration:Config):
        super().__init__("RoundRobin", configuration.mounted_file_share_name, configuration.log_identity)
        self.configuration = configuration

    def create_workloads(self) -> typing.List[str]:
        """
        Generate workload manfiests in the storage account identified by record_xxx fields
        by getting all of the unprocessed records from the storage table. 

        TODO: Break this up to minimum 1000 records per workload, but for now keep it simple
        for testing until it's all working. 
        """

        return_workloads = []

        # Get our logger
        logger:Logger = self.get_logger()

        logger.info("Record Storage Account: {}".format(self.configuration.record_account))
        logger.info("Record File Share: {}".format(self.configuration.record_account_share))
        record_share_util = FileShareUtil(
            self.configuration.record_account, 
            self.configuration.record_account_key, 
            self.configuration.record_account_share)

        ######################################################################
        # Storage table to track files
        table_util = AzureTableStoreUtil(
            self.configuration.record_account, 
            self.configuration.record_account_key)

        records:typing.List[Record] = table_util.search_unprocessed(self.configuration.record_storage_table)

        logger.info("Unprocessed Record Count: {}".format(len(records)))
        logger.info("Container Distribution: {}".format(self.configuration.container_count))

        if len(records) == 0:
            logger.warn("There are 0 records to process")
            print("There are 0 unprocessed records in the table.")
            return return_workloads

        # Create array, but we will limit the number of needed containers/workflow records
        # to at max self.configuration.container_count, but each container should take on at 
        # least 1000 records. 
        workloads = []
        container_count = int(self.configuration.container_count)
        record_count = len(records)
        containers_needed = int(record_count/1000)

        if container_count > containers_needed:
            container_count = containers_needed
            if container_count == 0 and record_count > 0:
                container_count = 1
            logger.info("Container Distribution Downgraded for {} records: {}".format(record_count, container_count))

        for idx in range(container_count):
            workloads.append([])
        
        # Round robin to different buckets
        count = 0
        for record in records:
            insert = count % container_count
            count += 1
            workloads[insert].append(record.RowKey)
        
        # Creat directory if needed
        record_share_util.create_directory(self.configuration.workload_path)

        for idx in range(len(workloads)):
            file_name = "workload{}.json".format(idx)
            logger.info("Generating work manifest: {}".format(file_name))
            
            with open(file_name, "w") as workload_manifest:
                workload_manifest.writelines(json.dumps(workloads[idx], indent=4))
            
            record_share_util.upload_file(self.configuration.workload_path, file_name)
            return_workloads.append(os.path.join(self.configuration.workload_path, file_name))
            os.remove(file_name)

        logger.info("Returning {} workloads".format(len(return_workloads)))
        
        return return_workloads