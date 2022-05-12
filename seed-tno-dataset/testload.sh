#!/bin/bash

##############################################################
# Set up the top settings to your deployment
# and open image (not ACR in this case) to test the container.
##############################################################

 # Set up the loader image locally
#/mnt/c/gitrepogrecoe/osdu-data-load/seed-tno-dataset/containers/seedosdu$ docker build -t anddang/seedosdu .

# Your sub where you have it deployed
AZURE_SUB="YOUR_AZURE_SUBSCRIPITON"
# Your RG of the deployment
AZURE_RG="AZURE_RG_WITH_OSDU_DEPLOYMENT"
# Storage account created from data move with file share populated
# with some data 
STORAGE_ACCOUNT="tnodataset6830"
# File share name in the storage account above
FILE_STORAGE_SHARE="tnodataset"
# The share mount location within the containers
SHARE_MOUNT_PATH="/mnt/tnodataset"
# The osdu data seed image Your own test container
LOAD_IMAGE="anddang/seedosdu"

# Build the dataload container
echo "Build the image and push it to dockerhub"
docker build -t $LOAD_IMAGE ./containers/seedosdu
docker push $LOAD_IMAGE

# Get remaining information from the RG itself
echo "Collect account information"
az account set -s $AZURE_SUB
TENANT=$(az account show --query tenantId -otsv)
KEYVAULT=$(az resource list -g $AZURE_RG --resource-type Microsoft.KeyVault/vaults --query [].name -otsv)
PLATFORM=$(az resource list -g $AZURE_RG --resource-type Microsoft.OpenEnergyPlatform/energyServices --query [].name -otsv)
CLIENT=$(az keyvault secret show --name client-id --vault-name $KEYVAULT --subscription $AZURE_SUB --query value -otsv)
SECRET=$(az keyvault secret show --name client-secret --vault-name $KEYVAULT --subscription $AZURE_SUB --query value -otsv)
STORAGE_KEY=$(az storage account keys list -g $AZURE_RG -n $STORAGE_ACCOUNT --query [0].value -otsv)


echo "Create ACI Instances"
for name in thdlgtest{1..8}
do 
    az container create \
        -g $AZURE_RG \
        --name $name \
        --image $LOAD_IMAGE \
        --cpu 4 \
        --memory 8 \
        --restart-policy Never \
        --azure-file-volume-share-name $FILE_STORAGE_SHARE \
        --azure-file-volume-account-name $STORAGE_ACCOUNT \
        --azure-file-volume-account-key $STORAGE_KEY \
        --azure-file-volume-mount-path $SHARE_MOUNT_PATH \
        --secure-environment-variables AZURE_TENANT=$TENANT EXPERIENCE_CLIENT=$CLIENT EXPERIENCE_CRED=$SECRET \
        --environment-variables SHARE_MOUNT=$SHARE_MOUNT_PATH ENERGY_PLATFORM=$PLATFORM
done


echo "Finished create ACI Instances"
