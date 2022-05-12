from utils.configuration.configutil import Config
from actions.scanaction import ScanAction
from actions.roundrobinaction import RoundRobin
from actions.workflowaction import WorkflowAction

# TODO: Upload logs to fileshare.

################################################################################
# General :
#  Load configuration from settings.ini or from environment 
configuration = Config("./settings.ini")

################################################################################
# Container 1 : Scanning
#   Scan Customer storage account using filters and record all files that have
#   not previously been added to the table (if any)
# Required Environment Parameters
#   DATA_SOURCE_ACCOUNT
#   DATA_SOURCE_ACCOUNT_KEY
#   DATA_SOURCE_ACCOUNT_SHARE
#   RECORD_STORAGE_ACCOUNT
#   RECORD_ACCOUNT_KEY
#   RECORD_SHARE_NAME
#   * TODO: DATA_PLATFORM
# Also required ./settings.ini -> LOAD, WORKLOADS
#
# TODO
#   Pass in platform name and use INI settings for ACL/Legal
#   Parrallize the processing of documents in Scan Action
#   Pass in ACR and OSDU Credentials to pass through to process container
#   Try and keep ALL activity to the launch script so no az login required.
#       Problem - az acr build requires an az acr login...how to resolve.
#       Identity on the container?
scan = ScanAction(configuration)
round_robin = RoundRobin(configuration)

# Scan storage and create metadata/table records for files
scan.scan_customer_storage()
# Generate a round robin approach to workloads sharing out all 
# unprocessed files across a number of files matchin the total
# number of containers that are being requested.
workloads = round_robin.create_workloads()

print("Created {} workloads".format(len(workloads)))
if len(workloads) == 0:
    print("No work to be done, exiting.....")
    quit()

# --> Create containers for each of the workloads recieved, passing the workload
# --> as another environment variable to the container.

################################################################################
# Container 1 : Worker 
#   Given the workload file name as a parameter, read  the file and find the 
#   records in the table store, create list of records and use parallel to 
#   process them as was done before.  
# Required Environment Parameters
#   RECORD_STORAGE_ACCOUNT
#   RECORD_ACCOUNT_KEY
#   RECORD_SHARE_NAME
#   * WORKLOAD_FILE --> NEW TRY USING ABOVE AS A START
#   TODO:
#   ACR /login server
#   OSDU Credentials for calling endpoints
#   DATA_PLATFORM
#   FILE_SHARE_NAME
# Also required ./settings.ini -> LOAD, WORKLOADS, CONNECTION, REQUEST

## DEBUG ONLY BECAUSE FILE SHOULD BE IN FILE SHARE
## Following code expects FILE_SHARE_MOUNT to be in environment as well
SHARE_MOUNT = "./outputwork"
LOCAL_WORKLOADS = []

import os
from utils.storage.share import FileShareUtil

print("DEBUG ONLY - Create Local workflows locally: {}".format(configuration.record_account_share))
record_share_util = FileShareUtil(
    configuration.record_account, 
    configuration.record_account_key, 
    configuration.record_account_share)


for workload in workloads:
    LOCAL_WORKLOADS.append(os.path.join(SHARE_MOUNT, workload))
    work = os.path.split(workload)
    record_share_util.download_file(SHARE_MOUNT, work[0], work[1])

print("Create local workloads done....")
print("DEBUG ONLY - Create Local workflows locally completed")
## DEBUG ONLY BECAUSE FILE SHOULD BE IN FILE SHARE
## Following code expects FILE_SHARE_MOUNT to be in environment as well

print("---> Mimic container workflow runs")
for loc in LOCAL_WORKLOADS:

    if os.path.exists(loc):
        ## DEBUG ONLY BECAUSE WORKFLOW SHOULD FLOW INTO THE ENVIRONMENT FROM CALLER
        os.environ["WORKFLOW_RECORD"] = loc
        ## DEBUG ONLY BECAUSE WORKFLOW SHOULD FLOW INTO THE ENVIRONMENT FROM CALLER

        configuration = Config("./settings.ini")

        workflow = WorkflowAction(configuration)
        workflow.process_records()
    
    print("Stopping with one workload")
    break

