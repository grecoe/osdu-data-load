#!/bin/bash

##############################################################
# Deploy container(s) to your subscription that have packaged
# the movebatch.py file to fan out execution.
##############################################################


# Your sub to deploy containers
AZURE_SUB="AZURE_SUB_ID_FOR_CONTAINERS"
# Your RG for container deployments
AZURE_RG="AZURE_RG_IN_SUB_FOR_CONTAINERS"
# The data movement image
LOAD_IMAGE="[YOURDOCKERHUBID]/batchload"

# Build the dataload container
echo "Build the image and push it to dockerhub"
docker build -t $LOAD_IMAGE .
docker push $LOAD_IMAGE

az account set -s $AZURE_SUB

echo "Create ACI Instances"
for name in movetest{1..4}
do 
    az container create \
        -g $AZURE_RG \
        --name $name \
        --image $LOAD_IMAGE \
        --cpu 4 \
        --memory 4 \
        --restart-policy Never 
done


echo "Finished create ACI Instances"
