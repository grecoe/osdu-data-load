# Bulk Data Loading

A process of loading and tracking a large number of files into the OSDU instance. 

# Requirements

- Separate Azure Storage Account with a File Share hosting data to be pushed to the ODSU system. 
- An OSDU instance that has been pre-created in Azure. 

## Azure Requirements
- Azure subscription(s) in which to host an Azure Storage account. 
- Azure subscription in which an OSDU instance can be deployed. 

## Customer Requirement
The customer fills out the details in the __custinput.sh__ file. It contains information about the file share that they have created for a data set as well as information about the OSDU instance they have deployed into their subscription. 

# Execution

./custrun.sh custinput.sh

-f identifiy the file name

docker build -f ctx/Dockerfile http://server/ctx.tar.gz


# Containers