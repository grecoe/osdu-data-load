from utils.configuration.configutil import Config
from actions.scanaction import ScanAction
from actions.roundrobinaction import RoundRobin
from actions.workflowaction import WorkflowAction


################################################################################
################################################################################
# DEBUG/TESTING CODE  
# Two use cases to cover for local testing. 
# asf
# 1. Validation: Proves certain items are in the environment for each of the types
#    of containers - LOAD and WORKFLOW that need to be created. 
# 2. Workflow Manifest Download: Manfiests are generated on the storage account but
#    the workflow needs the file local, so mimic it by moving the file into the local
#    system where that code can reach it. 
#
import os
from utils.storage.share import FileShareUtil

def verify_required_environment(expected_entries:list):
    for entry in expected_entries:
        if entry not in os.environ:
            raise Exception("{} required for this stage".format(entry))


def get_workloads_locally(configuration:Config, workloads:list) -> list:
    mock_share_mount = "./outputwork"
    return_local_workloads = []
    
    record_share_util = FileShareUtil(
        configuration.record_account, 
        configuration.record_account_key, 
        configuration.record_account_share)

    for workload in workloads:
        work = os.path.split(workload)
        return_local_workloads.append(os.path.join(mock_share_mount, work[1]))
        record_share_util.download_file(mock_share_mount, work[0], work[1])

    return return_local_workloads

# End local testing requirements
################################################################################
################################################################################

def workload_container_execute():
    """
    Code needed to execute the workload container, complete with a list of 
    required environment variables outside of the INI settings file. 
    """

    print("DEBUG - Executing Workload Container")

    # Precursor: This code does NOT live in the actual container. However, the 
    # container will FAIL if any of these settings OR the INI file are missing
    verify_required_environment ( 
        [
            "DATA_PLATFORM",
            "RECORD_STORAGE_ACCOUNT",
            "RECORD_ACCOUNT_KEY",
            "RECORD_SHARE_NAME",
            "FILE_SHARE_NAME",
            "WORKFLOW_RECORD",
            "PLATFORM_TENANT",
            "PLATFORM_CLIENT",
            "PLATFORM_SECRET"
        ]
    )
    # END Precursor

    configuration = Config.get_workflow_configuration("./settings.ini")

    workflow = WorkflowAction(configuration)
    workflow.process_records()    

def load_container_execute():
    """
    Code needed to execute the load container, complete with a list of 
    required environment variables outside of the INI settings file. 

    TODO: Have to create/launch ACI instances for Workload Containers
    """

    print("DEBUG - Executing Load Container")

    # Precursor: This code does NOT live in the actual container. However, the 
    # container will FAIL if any of these settings OR the INI file are missing
    verify_required_environment ( 
        [
            "DATA_PLATFORM",
            "RECORD_STORAGE_ACCOUNT",
            "RECORD_ACCOUNT_KEY",
            "RECORD_SHARE_NAME",
            "FILE_SHARE_NAME",
            "DATA_SOURCE_ACCOUNT",
            "DATA_SOURCE_ACCOUNT_KEY",
            "DATA_SOURCE_ACCOUNT_SHARE",
            "DATA_SOURCE_MAP"
        ]
    )
    # END Precursor


    # Start Container Code
    # NOTE: Running locally this code is doing a few rounds getting the workflow
    #       records to within reach of the local code in the if len(workloads) else
    #       code block. 
    configuration = Config.get_load_configuration("./settings.ini")

    # Create actions to perform
    scan = ScanAction(configuration)
    round_robin = RoundRobin(configuration)

    # Scan storage and create metadata/table records for files
    scan.scan_customer_storage()
    # Generate workload manifests 
    workloads = round_robin.create_workloads()

    if len(workloads) == 0:
        print("Scan detected no work to be completed this run")
    else:
        # DEBUG - This should just execute the workload conainer, but we need to 
        # get the files locally. 
        local_workloads = get_workloads_locally(configuration, workloads)

        # Loop is also debug because each workload container will get the manifest to work
        # on as an environment variable. 
        for loc in local_workloads:
            if os.path.exists(loc):

                print("*** Launch Workload Container - Replace with ACI Instance ***")
                # Will be passed along with other environment settings
                os.environ["WORKFLOW_RECORD"] = loc
                # Execute Workload Container (needs to be created)
                workload_container_execute()
            
            print("***** Stopping with one workload *****")
            break


############################################################################
# Execution, the load container will be kicked by custrun.sh or whatever it
# becomes. It internally will generate the workload containers to process
# what it finds. 
print("DEBUG - Simulation started - DEBUG")

load_container_execute()

print("DEBUG - Simulation complete - DEBUG")
