# Seed TNO Dataset

Project is comprised of two containers

## Load
Given a sub id and resource group, the container will
- Log in a user (manual)
- Create a storage account with a file share and copy the public TNO data into it.
- Create a new container for validation with the file share mounted in the container.

<NOTE> In containers/datamovement/loaddataset.sh currently an issue with getting an Expiry value from the system that doesn't cause getting a SAS token to fail. 

## Validate
Simply validates that the mount was created and there is something in it. 

# Deploying
Have docker installed and be logged into your dockerhub account. 

Open a bash shell to this directory

Modify *demonstrate.sh* for these settings

AZURE_SUBSCRIPTION="YOUR_SUB_ID"
AZURE_RESOURCE_GROUP="YOUR_RG_TO_DEPLOY_EXISTS"
DATAMOVEMENT_IMAGE="YOURDHNAME/dataloading"
VALIDATION_IMAGE="YOURDHNAME/datavalidation"