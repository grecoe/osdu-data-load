import os
import json
from textwrap import indent
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
            return return_workloads

        # Create array
        workloads = []
        for idx in range(int(self.configuration.container_count)):
            workloads.append([])
        
        # Round robin to different buckets
        count = 0
        for record in records:
            insert = count % int(self.configuration.container_count)
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

        return return_workloads