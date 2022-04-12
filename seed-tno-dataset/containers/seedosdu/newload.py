from distutils.command.upload import upload
import os
import typing
import json
from utils.fileshare.mount import FileClass, Mount
from utils.logutil import ActivityLog


DRIVE_MOUNT = "Q:\\\\"

upload_activity = ActivityLog(DRIVE_MOUNT, "test")

classes:typing.List[FileClass] = [
    FileClass(["TNO"], "md"),
    FileClass(["/schema/type"], "json")
]

classes2:typing.List[FileClass] = [
    FileClass(["markers"], "csv")
]

upload_activity.add_activity("Loading files")
Mount.load_files(classes2, DRIVE_MOUNT)
upload_activity.add_activity("Done loading files")

upload_activity.add_activity("Identity", classes2[0].identity)
upload_activity.add_activity("Active Path", classes2[0].loaded_paths)

for cls in classes2:
    print(cls.identity)
    print(cls.parent_dir)
    print(cls.data_extension)
    print(cls.supported_paths)
    print(cls.loaded_paths)
    print(len(cls.files))

upload_activity.dump()