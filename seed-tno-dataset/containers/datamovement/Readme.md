# Datamovement

This container retrieves the public data for the project, creates a storage account and copies that data into a file share in that account. 

When done, it creates another container instance and mounts the file share so that it will have access to process the data. 

### Required Environment Variables:

|Variable|Content|
|--------|-------|
|SUBSCRIPTION|Your Azure Subscription ID|
|RESOURCE_GROUP|Your Azure Resource Group in the given Subscription that holds the OSDU deployment.|
|VALIDATION_IMAGE|The image (from the same ACR) to do the actual loading of data|

## Actions
- Create a storage account 
- Create a file share in the storage account
- Unpack azcopy
- Move files from the designated location to the file share
- Launch the [seedosdu](../seedosdu/Readme.md) container

## Logging
None other than output from the shell script visible in the Portal for the ACI. 