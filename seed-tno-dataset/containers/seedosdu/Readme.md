# Seed OSDU 

This container is connected with a file share and reads all of the files within that share and loads them into the OSDU instance. 

Files are selected using the FileClass class

Each of them get the same metadata associated, but now we have them by folder, we can then determine if they need to be modified/udpated before being pushed and even modify the contents of the metadata based on what is being sent. 

### Required Environment Variables:

|Variable|Content|
|--------|-------|
|SHARE_MOUNT|The file share mount location.|
|ENERGY_PLATFORM|Your OSDU Deployment name, used in metadata and determining partition.|
|AZURE_TENANT|(SECURE) The Azure Tenant of the Application given rights to the OSDU deployment.|
|EXPERIENCE_CLIENT|(SECURE) The Application ID|
|EXPERIENCE_CRED|(SECURE) The Application secret|

## Actions
1. Filter the attached file share using the FileClass and Mount classes to filter files for specific locations. 
2. For each file found
    - Get an upload URL from OSDU
    - Upload the file
    - Upload a metadata packet for that file (utils/requests/metagenerator.py)


## Logging
As this container requires a file share mounted to work, two different log files are generated at the root of the mount. 

|File|Contents|
|----|-------|
|/outputs/dataloader-XXX.log|The full log generated from all of the components running.|
|/activity/dataload-XXX.log|A summary activity log of what happened during the run.|

<sub>XXX is a GUID if settings.ini/logging/use_identity is True, otherwise it's a date stamp of the day it runs.</sub>

> <b>NOTE:</b> While there is only one activity log generated, you can generate multiple activity logs at any time utilizing the ActivityLog class.