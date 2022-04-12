# osdu-data-load

Example on loading data to an OSDU system in a managed instance.

The project loads open source TNO data from the Energy Sector and comes from the OSDU Open Community. 

## Requirements
- Azure Subscription 
- Managed OSDU Instance Deployed


## Functionality

The functionality is broken down into two Docker containers that are built in Azure Container Registry and served up by Azure Container Instances. 

Containers can be found under seed-tno-dataset/containers

### datamovement
This one runs through the following steps

- Logs the user into Azure 
- Creates a storage account
    - Creates a File Share in that storage account
- Utilizes AZ COPY to copy the open source data to the file share
- Launches the second (seedosdu) container.

### seedosdu
This process loads up files from the file share which has been mounted by the datamovement container before launching and pushes the files, with metadata, to the OSDU instance using standard OSDU API's. 
