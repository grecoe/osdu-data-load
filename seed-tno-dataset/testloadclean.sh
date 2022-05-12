#!/bin/bash

# Clean up all of the instances of load that were generated with testload.sh

# Your sub where you have it deployed
AZURE_SUB="YOUR_AZURE_SUBSCRIPITON"
# Your RG of the deployment
AZURE_RG="AZURE_RG_WITH_OSDU_DEPLOYMENT"

# Make this pattern the same as containers created with testload.sh so you can 
# easily figure out if a container in the batch is still running.
TEMPLATE="thdlgtest"

az account set -s $AZURE_SUB

CONTAINERS=$(az resource list -g $AZURE_RG --resource-type Microsoft.ContainerInstance/containerGroups --query [].name -otsv)

echo "Scanning ${#CONTAINERS[@]} in group $AZURE_RG"
count=1
for cont in $CONTAINERS
do

    state=$(az container show -n $cont -g $AZURE_RG --query instanceView.state -otsv)
    
    if [[ $state = "Running" ]]; then
        echo "$count $cont is running"
    else
        echo "$count $cont is stopped"
        if [[ $cont == *"$TEMPLATE"* ]]; then
            echo "$cont matches the format $TEMPLATE and will be deleted"
            az container delete -n $cont -g $AZURE_RG -y > /dev/null
        fi
    fi

    count=$((count + 1))
done

