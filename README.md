# osdu-data-load

Example on loading data to an OSDU system in a managed instance.

The project loads open source TNO data from the Energy Sector and comes from the OSDU Open Community. 

## Requirements
- Azure Subscription 
    - You will need the Subscription ID
- Managed OSDU Instance Deployed
    - You will need the Resource Group name in which it is deployed.


## Functionality

- [Copy public files to a file share](./seed-tno-dataset/containers/datamovement/Readme.md)
- [Move files from share to OSDU](./seed-tno-dataset/containers/seedosdu/Readme.md)
