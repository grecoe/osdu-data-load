#!/bin/bash

#######################################################
# Required Environment Variables
#######################################################
AZURE_SUB=$(printenv SUBSCRIPTION)
AZURE_RG=$(printenv RESOURCE_GROUP)
DATASEED_IMAGE=$(printenv DATA_SEED_IMAGE)
ACR_REGISTRY=$(printenv ACR)

#######################################################
# Settings for file share
#######################################################
RANDOM_NUMBER=$(echo $((RANDOM%9999)))
DATA_STORAGE_ACCOUNT="tnodataset"$RANDOM_NUMBER
FILE_STORAGE_SHARE="tnodataset"

#######################################################
# Settings for AZ Copy
#######################################################
#TNOFULLDATASET="https://oaklabexperience.blob.core.windows.net/open-test-data"

# WAS the set I was using but it changed.....most of the directories are the same BUT
# I think the documents changed....but I can't see the container contents now in the 
# new blob container:)
#TNODATASET="https://azureingestiondata.blob.core.windows.net/tno-datasets"

TNODATASET="https://opentestdatatest.blob.core.windows.net/tno-dataset"
AZCOPYLOCATION="./azcopy"
AZCOPYTAR=$(ls $AZCOPYLOCATION)


#######################################################
# Set the account
#######################################################
echo "Login and set account to $AZURE_SUB"
az login --use-device-code
az account set -s $AZURE_SUB > /dev/null

#######################################################
# Create the target storage account to load to
#######################################################
echo "Creating storage account $DATA_STORAGE_ACCOUNT in group $AZURE_RG"
az storage account create -n $DATA_STORAGE_ACCOUNT -g $AZURE_RG \
   --allow-blob-public-access false \
   --kind StorageV2 \
   --sku Standard_ZRS

echo "Get storage account key for $DATA_STORAGE_ACCOUNT"
STORAGE_KEY=$(az storage account keys list -n $DATA_STORAGE_ACCOUNT -g $AZURE_RG --query [0].value -otsv)

echo "Create file share $FILE_STORAGE_SHARE"
az storage share create --name $FILE_STORAGE_SHARE \
   --account-key $STORAGE_KEY \
   --account-name $DATA_STORAGE_ACCOUNT

echo "Get file share SAS Token"
SAS_EXPIRATION=`date -u -d "@$(( $(busybox date +%s) + 3600 ))" '+%Y-%m-%dT%H:%MZ'`
SHARE_SAS=$(az storage share generate-sas -n $FILE_STORAGE_SHARE --account-name $DATA_STORAGE_ACCOUNT --https-only --permissions dlrw --expiry $SAS_EXPIRATION -otsv)


FILE_SHARE_SAS="https://$DATA_STORAGE_ACCOUNT.file.core.windows.net/$FILE_STORAGE_SHARE/?$SHARE_SAS"


#######################################################
# Extract AZCOPY and move files from public to share
#######################################################
echo "Extract AZCOPY"
tar xf $AZCOPYLOCATION/$AZCOPYTAR -C $AZCOPYLOCATION
AZCOPYINSTALLED=$(find $AZCOPYLOCATION/* -type d)
AZCOPYINSTALLED=$AZCOPYINSTALLED"/azcopy"
echo $AZCOPYINSTALLED

echo "Move datasset"
$AZCOPYINSTALLED copy $TNODATASET $FILE_SHARE_SAS --recursive

echo "Dataset: $TNODATASET"
echo "Copy Location:"
echo "   Subscription   : $AZURE_SUB"
echo "   Resource Group : $AZURE_RG"
echo "   Storage Account: $DATA_STORAGE_ACCOUNT"
echo "   File Share     : $FILE_STORAGE_SHARE"

#######################################################
# Create the container to validate it all
# 1. Add in the getting of the MSI, setting key permissions, and adding to this container
# 2. Look in follow on container for use of the environment variables. Do we need anything
#    else? If so, should we just pass the environment variables here?
#######################################################

#######################################################
# NEW Give MSI Get access to keys
#######################################################
TENANT=$(az account show --query tenantId -otsv)
KEYVAULT=$(az resource list -g $AZURE_RG --resource-type Microsoft.KeyVault/vaults --query [].name -otsv)
PLATFORM=$(az resource list -g $AZURE_RG --resource-type Microsoft.OpenEnergyPlatform/energyServices --query [].name -otsv)

CLIENT=$(az keyvault secret show --name client-id --vault-name $KEYVAULT --subscription $AZURE_SUB --query value -otsv)
SECRET=$(az keyvault secret show --name client-secret --vault-name $KEYVAULT --subscription $AZURE_SUB --query value -otsv)

ACR_LOGIN_SERVER=$(az acr show --name $ACR_REGISTRY --resource-group $AZURE_RG --query loginServer -otsv)
MANAGED_IDENTITY=$(az resource list -g $AZURE_RG --resource-type Microsoft.ManagedIdentity/userAssignedIdentities --query [].id -otsv)

#######################################################
# NEW
#######################################################

echo "Create container instance for image $DATASEED_IMAGE"
SHARE_MOUNT_PATH="/mnt/tnodataset"
CONTAINER_NM_ACR="tnoseedosdu"
az container create \
    -g $AZURE_RG \
    --name  $CONTAINER_NM_ACR \
    --image $DATASEED_IMAGE \
    --cpu 4 \
    --memory 4 \
    --restart-policy Never \
    --registry-login-server $ACR_LOGIN_SERVER \
    --acr-identity $MANAGED_IDENTITY \
    --assign-identity $MANAGED_IDENTITY \
    --azure-file-volume-share-name $FILE_STORAGE_SHARE \
    --azure-file-volume-account-name $DATA_STORAGE_ACCOUNT \
    --azure-file-volume-account-key $STORAGE_KEY \
    --azure-file-volume-mount-path $SHARE_MOUNT_PATH \
    --secure-environment-variables AZURE_TENANT=$TENANT EXPERIENCE_CLIENT=$CLIENT EXPERIENCE_CRED=$SECRET \
    --environment-variables SHARE_MOUNT=$SHARE_MOUNT_PATH ENERGY_PLATFORM=$PLATFORM

echo "Container instance created for image $DATASEED_IMAGE"
exit  