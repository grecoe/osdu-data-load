AZURE_SUB="YOUR_AZURE_SUBSCRIPITON"
AZURE_RG="AZURE_RG_WITH_OSDU_DEPLOYMENT"
DATAMOVEMENT_IMAGE="open-test-data/tno-acquire-data:demo"
LOAD_IMAGE="open-test-data/tno-seed-osdu:demo"

az account set -s $AZURE_SUB

# Get container registry information
echo "Collect information from resource group $AZURE_RG"
ACR_REGISTRY=$(az resource list -g $AZURE_RG --resource-type Microsoft.ContainerRegistry/registries --query [].name -otsv)
ACR_LOGIN_SERVER=$(az acr show --name $ACR_REGISTRY --resource-group $AZURE_RG --query loginServer -otsv)
MANAGED_IDENTITY=$(az resource list -g $AZURE_RG --resource-type Microsoft.ManagedIdentity/userAssignedIdentities --query [].id -otsv)

##############################################
# Build validation container and push it
##############################################
echo "Build data seeding image: $LOAD_IMAGE in ACR $ACR_REGISTRY"
az acr build -t $LOAD_IMAGE -r $ACR_REGISTRY -g $AZURE_RG ./containers/seedosdu/

##############################################
# Build data movement container
##############################################
echo "Build data movement image: $DATAMOVEMENT_IMAGE in ACR $ACR_REGISTRY"
az acr build -t $DATAMOVEMENT_IMAGE -r $ACR_REGISTRY -g $AZURE_RG ./containers/datamovement/


##############################################
# Now crate and launch the data movement 
##############################################
ACR_DATAMOVEMENT_IMAGE="$ACR_LOGIN_SERVER/$DATAMOVEMENT_IMAGE"
ACR_LOAD_IMAGE="$ACR_LOGIN_SERVER/$LOAD_IMAGE"

CONTAINER_NAME="tnoloadosdu"
az container create \
    -g $AZURE_RG \
    --name $CONTAINER_NAME \
    --cpu 2 \
    --image $ACR_DATAMOVEMENT_IMAGE \
    --registry-login-server $ACR_LOGIN_SERVER \
    --acr-identity $MANAGED_IDENTITY \
    --assign-identity $MANAGED_IDENTITY \
    --restart-policy Never \
    --environment-variables DATA_SEED_IMAGE=$ACR_LOAD_IMAGE ACR=$ACR_REGISTRY SUBSCRIPTION=$AZURE_SUB RESOURCE_GROUP=$AZURE_RG
