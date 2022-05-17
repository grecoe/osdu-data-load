##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import os
import typing
from utils.configuration.config import Config
from utils.requests.auth import Credential
from utils.uploader import FileUploader, UploadResults
from utils.logutil import LoggingUtils, ActivityLog
from utils.fileshare.mount import FileClass, Mount


################################################
# Load Configuration (environment and ini file)
################################################
config = Config("./settings.ini")
config.logger = LoggingUtils.get_logger(config.file_share_mount, config.log_name, config.log_identity)

################################################
# Load files from file mount
################################################
print("Mount path provided: ", config.file_share_mount)
print("Mount path exists: ", os.path.exists(config.file_share_mount))

activity_log = ActivityLog(config.file_share_mount, "dataload", config.log_identity)

classes:typing.List[FileClass] = [
    FileClass(["markers"], "csv"),
    FileClass(["trajectories"], "csv"),
    FileClass(["documents"], "pdf"),
    FileClass(["well-logs"], "las"),
]

activity_log.add_activity("Filter share mount for files : {}".format(config.file_share_mount))
Mount.load_files(classes, config.file_share_mount)
activity_log.add_activity("Finished filtering share mount")



################################################
# Get credentials to make calls to OSDU
################################################
config.logger.info("Acquire credentials")
appCred = Credential(config)

################################################
# Upload files in batches of files
################################################
report:typing.List[str] = []
for c in classes:

    config.logger.info("Upload Files - {}".format(c.parent_dir))
    report.append("Starting {} - {} files".format(c.parent_dir, len(c.files)))


    if len(c.files):
        activity_log.add_activity("Upload files from {} : {}".format(c.parent_dir, len(c.files)))
    
        uploader = FileUploader(c.files, config, appCred)
        results:UploadResults = uploader.upload_files()

        activity_log.add_activity("Succesful Uploads: {}".format(len(results.success)))
        activity_log.add_activity("Failed Uploads: {}".format(len(results.failed)))

        report.append("Success: {}".format(len(results.success)))
        report.append("Failed: {}".format(len(results.failed)))

for rep in report:
    config.logger.info(rep)


config.logger.info("Completed")
activity_log.add_activity("Completed dataload process")
activity_log.dump()

