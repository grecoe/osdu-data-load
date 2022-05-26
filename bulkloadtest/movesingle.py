##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################

"""
Install AZ Copy somewhere on the machine

Provide two SAS tokeninzed URI's to locations within storage accounts

Unpack AZCOPY locally
- Open bash script and point to this path
- Unpack azcopy
    tar -xzvf azcopy_linux_amd64_10.14.1.tar.gz --strip-components=1 

Observations
    - 100.99GB file in 260 seconds / 4.33 minutes 
"""
import os
import json
from bulkloadtest.utils.commandline import CmdUtils
from datetime import datetime, timedelta

class StorageFile:
    def __init__(self, url:str, account_sas:str):
        self.url = url
        self.account_sas = account_sas

    def get_url(self):
        return_url = self.url
        if self.account_sas:
            return_url += self.account_sas
        return return_url

class CopyResult:
    def __init__(self, success:bool=False, copy_time:float = 0.0):
        self.success = success
        self.minutes = copy_time

class StorageCopy:

    @staticmethod
    def copy_storage_file(azcopy_path:str, source:StorageFile, destination:StorageFile) -> CopyResult:
        copied = CopyResult()

        try:
            output = CmdUtils.get_command_output(
                [
                    azcopy_path, 
                    "copy", 
                    source.get_url(), 
                    destination.get_url()
                ], 
            False, 
            False)

            output = output.split(os.linesep)

            target = [x for x in output if "Total Number of Transfers" in x]
            result = [x for x in output if "Number of Transfers Completed" in x]
            time = [x for x in output if "Elapsed Time (Minutes)" in x]

            expected = StorageCopy._parse_result(target)
            moved = StorageCopy._parse_result(result)
            minutes = StorageCopy._parse_result(time, float)

            if expected != moved:
                raise Exception("Attempted: {} , Moved: {}".format(expected, moved))

            copied.success = True
            copied.minutes = minutes

        except Exception as ex:
            print("Copy Error: ", str(ex))

        return copied 

    @staticmethod
    def _parse_result(result:str, klass = int) -> int:
        return_val = None
        if len(result):
            result = result[0].split(":")
            if len(result) == 2:
                return_val = klass(result[1].strip())

        return return_val        


AZCOPY_PATH = "./azcopy"

# Source file to move
SOURCE_FILE = "https://ACCOUNT.file.core.windows.net/SHARE/PATH/FILE"
SOURCE_SAS = "?SAS_TOKEN"

# Destination file to create
DESTINATION_FILE = "https://ACCOUNT.file.core.windows.net/SHARE/PATH/FILE"
DESTINATION_SAS = "?SAS_TOKEN"


print("*** Start copy ***")
start:datetime = datetime.utcnow()

result = StorageCopy.copy_storage_file(
    AZCOPY_PATH, 
    StorageFile(SOURCE_FILE, SOURCE_SAS),
    StorageFile(DESTINATION_FILE, DESTINATION_SAS),
)

end:datetime = datetime.utcnow()
delta:timedelta = end-start

print("*** Copy complete in {} seconds ***".format(delta.total_seconds()))
print(json.dumps(result.__dict__, indent=4))

