#!/bin/bash


# Input of custinput.sh is required, or any shell file that meets the goal of supplying those fields. 

if [[ -z $1 ]];then
    echo "Input file is required"
fi

source "$1"

##############################################################
# Get the required values from the environment from the customer
# information passed in with custinput.sh or whatever it gets
# named. 
echo "Collecting information about the data set file share"
DATA_SOURCE_SUBCRIPTION=$(printenv DATA_SOURCE_SUBCRIPTION)
DATA_SOURCE_RESOURCE_GROUP=$(printenv DATA_SOURCE_RESOURCE_GROUP)
DATA_SOURCE_ACCOUNT=$(printenv DATA_SOURCE_ACCOUNT)
DATA_SOURCE_ACCOUNT_SHARE=$(printenv DATA_SOURCE_ACCOUNT_SHARE)
export DATA_SOURCE_ACCOUNT_KEY=$(az storage account keys list -g $DATA_SOURCE_RESOURCE_GROUP -n $DATA_SOURCE_ACCOUNT --subscription $DATA_SOURCE_SUBCRIPTION --query [0].value -otsv)
DATA_SOURCE_ACCOUNT_KEY=$(printenv DATA_SOURCE_ACCOUNT_KEY)

echo "Collecting Information from OSDU Deployment"
EXPERIENCE_LAB_SUBSCRIPTION=$(printenv EXPERIENCE_LAB_SUBSCRIPTION)
EXPERIENCE_LAB_RESOURCE_GROUP=$(printenv EXPERIENCE_LAB_RESOURCE_GROUP)


##############################################################
# Collect information about the user and the OSDU instance they
# have created. 
TENANT=$(az account show --query tenantId -otsv)
KEYVAULT=$(az resource list -g $EXPERIENCE_LAB_RESOURCE_GROUP --subscription $EXPERIENCE_LAB_SUBSCRIPTION --resource-type Microsoft.KeyVault/vaults --query [].name -otsv)
PLATFORM=$(az resource list -g $EXPERIENCE_LAB_RESOURCE_GROUP --subscription $EXPERIENCE_LAB_SUBSCRIPTION --resource-type Microsoft.OpenEnergyPlatform/energyServices --query [].name -otsv)

CLIENT=$(az keyvault secret show --name client-id --vault-name $KEYVAULT --subscription $EXPERIENCE_LAB_SUBSCRIPTION --query value -otsv)
SECRET=$(az keyvault secret show --name client-secret --vault-name $KEYVAULT --subscription $EXPERIENCE_LAB_SUBSCRIPTION --query value -otsv)

ACR_REGISTRY=$(az resource list -g $EXPERIENCE_LAB_RESOURCE_GROUP --subscription $EXPERIENCE_LAB_SUBSCRIPTION --resource-type Microsoft.ContainerRegistry/registries --query [].name -otsv)
ACR_LOGIN_SERVER=$(az acr show --name $ACR_REGISTRY --resource-group $EXPERIENCE_LAB_RESOURCE_GROUP --subscription $EXPERIENCE_LAB_SUBSCRIPTION --query loginServer -otsv)
MANAGED_IDENTITY=$(az resource list -g $EXPERIENCE_LAB_RESOURCE_GROUP --subscription $EXPERIENCE_LAB_SUBSCRIPTION --resource-type Microsoft.ManagedIdentity/userAssignedIdentities --query [].id -otsv)


##############################################################
# Need to generate a storage account in the OSDU deployment 
# resource group. This will be used to host logs and other 
# outputs from the follow on containers.
RANDOM_STG_IDENTIFIER=$(echo $((RANDOM%9999)))

##############
# Replace with actual account creation, last 4 are really hard codes and not needed to create share
RECORD_STORAGE_ACCOUNT="tnodataset6830"
RECORD_ACCOUNT_KEY="THNNz/w3p4RfzxkvN4doGWVirrL+qj8A+U7Bt+ce9aItB1iIR7LGjy9dYkw55OQgS/sPvVDS5+FEXBNe8cXFtQ=="
RECORD_SHARE_NAME="record"
FILE_SHARE_NAME="./"

##############################################################
# Exported values for follow on processes, these values combined
# with some from the custinput.sh are required to run the processes
#
# Replace calling the test python directly and instead create/
# launch the Load Container (first part of that follow on file)
# which will then have enough information to create the workflow
# containers. 
#
# The Load container will also have a linked in file share for 
# it's logs. 
export RECORD_STORAGE_ACCOUNT=$RECORD_STORAGE_ACCOUNT
export RECORD_ACCOUNT_KEY=$RECORD_ACCOUNT_KEY
export RECORD_SHARE_NAME=$RECORD_SHARE_NAME
export DATA_PLATFORM=$PLATFORM
export FILE_SHARE_NAME=$FILE_SHARE_NAME

export PLATFORM_TENANT=$TENANT
export PLATFORM_CLIENT=$CLIENT
export PLATFORM_SECRET=$SECRET

echo "*** Launch Script - Replace with ACI Instance LOAD CONTAINER ***"
python custtest.py
