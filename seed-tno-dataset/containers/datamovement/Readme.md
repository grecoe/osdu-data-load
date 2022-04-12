# Datamovement

This container retrieves the public data for the project, creates a storage account and copies that data into a file share in that account. 

When done, it creates another container instance and mounts the file share so that it will have access to process the data. 

Required Environment Variables:

SUBSCRIPTION=SUBSCRIPTION_ID
RESOURCE_GROUP=AZURE_RG_NAME_LAB
VALIDATION_IMAGE=IMAGE_FOR_VALIDATION