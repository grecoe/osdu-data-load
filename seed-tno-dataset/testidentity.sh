# Set key vault policy to the MSI in teh RG

##############################################################
# Dummy script to do some minor testing
##############################################################

SUBSCRIPTION="YOUR_SUB_ID"
AZURE_RESOURCE_GROUP="experiencelab-ALIAS"


az account set -s $SUBSCRIPTION


USER=$(az ad signed-in-user show --query objectId -otsv)
TENANT=$(az account show --query tenantId -otsv)
KEYVAULT=$(az resource list -g $AZURE_RESOURCE_GROUP --resource-type Microsoft.KeyVault/vaults --query [].name -otsv)
#CLIENT=$(az keyvault secret show --name client-id --vault-name $KEYVAULT --query value -otsv)
#SECRET=$(az keyvault secret show --name client-secret --vault-name $KEYVAULT --query value -otsv)
PLATFORM=$(az resource list -g $AZURE_RESOURCE_GROUP --resource-type Microsoft.OpenEnergyPlatform/energyServices --query [].name -otsv)
IDENTITY=$(az resource list -g $AZURE_RESOURCE_GROUP --resource-type Microsoft.ManagedIdentity/userAssignedIdentities --query [0].name -otsv)
IDENTITY_ID=$(az identity show --name $IDENTITY --resource-group $AZURE_RESOURCE_GROUP --query principalId -otsv)

# az identity show --name experiencelab9529 --resource-group	experiencelab-grecoe
echo $KEYVAULT
echo $IDENTITY
echo $IDENTITY_ID

# Set the managed identity as someone who can read the vault
echo "Set vault pokicy for MSI"
az keyvault set-policy \
    -n $KEYVAULT \
    -g $AZURE_RESOURCE_GROUP \
    --secret-permissions get \
    --object-id $IDENTITY_ID
