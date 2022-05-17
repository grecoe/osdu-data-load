# OSDU Bulk Data Loading
<sub>Dan Grecoe is a Microsoft Employee</sub>

A generic process of loading and tracking a large number of files into the OSDU instance. 

<b>Contents</b>
- [Requirements](#requirements)
    - [Customer Requirements](#customer-requirements)
- [Execution](#execution)
    - [Work flow](#flow)
    - [Storage Table Records](#storage-table-record)
- [Next Steps](#next-steps)


# Requirements

- At least one Azure Subscription
- Configure an Azure Storage Account with a File Share with some dataset to ingest. This is the data to push to the OSDU system. 
- An OSDU instance in Azure
- Complete the system/service settings in __custinput.sh__ see [Customer Requirement](#customer-requirement)


## Customer Requirements
The customer fills out the details in the __custinput.sh__ file. It contains information about the file share that they have created for a data set as well as information about the OSDU instance they have deployed into their subscription. 

# Execution
To execute this test locally, open up __custinput.sh__ and change the values for all DATA_SOURCE_* fields to a sub/rg/storage acct/file share with the files to upload. Note the path of the files in the share and update DATA_SOURCE_MAP to files you want to ingest. 

Modify the other settings to your OSDU deployment Sub/RG. 

Open up a bash shell to this directory and run the following:

```bash
conda env create -f environment.yml
conda activate Datascan
az login
./custrun.sh custinput.sh
```

<b>Sub Sections</b>
- [Work flow](#flow)
- [Storage Table Records](#storage-table-record)

## Flow

<table>
    <tr> <td colspan=2> Customer Data Acquisition</td></tr>
    <tr>
        <td width="40%">
            <ol type="1">
                <li>Customer acquires data set.</li>
                <li>Customer loads dataset into Azure Storage File Share into any subscription they choose.</li>
            </ol>
        </td>
        <td>
            <img width="100%" src="./docs/acquire_data.jpg"/>
        </td>
    </tr>
    <tr> <td colspan=2> Customer Initiated Load</td></tr>
    <tr>
        <td width="40%">
            <ol type="1">
                <li>Customer deploys OSDU instane into any subscription they choose.</li>
                    <ul>
                        <li> User must have access to the subscripiton with the file share in it.
                    </ul>
                <li>Customer collects information about the dataset file share and OSDU deployment and modifies the settings file <b>custinput.sh</b> </li>
                <li>Customer logs into Azure Cloud Shell and executes the script. Script will launch the load container after required resource data has been collected.</li>
                    <ul>
                        <li> ./custrun.sh custinput.sh
                        <li> Very rough start here, currently runs locally. Need better solution for execution
                    </ul>
            </ol>
        </td>
        <td>
            <img width="100%" src="./docs/execute_script.jpg"/>
        </td>
    </tr>
    <tr> <td colspan=2> Load Container</td></tr>
    <tr>
        <td width="40%">
            <ol type="1">
                <li>Creates Azure Storage Account in Resource Group containing OSDU instance, then scans customer file share using filters set up in the input script.</li>
                <li>Generates a metadata record for the file and stores into the newly created file share from step 1. </li>
                <li>Creates a record with the SAS URL to the original file, and location of the metadata record in storage from step 1.</li>
                <li>Creates a workflow manifest file for each work container to launch. Then, for each manifest launches a workload container to manage the workflow.</li>
            </ol>
        </td>
        <td>
            <img width="100%" src="./docs/load_container.jpg"/>
        </td>
    </tr>
    <tr> <td colspan=2> Workload Container</td></tr>
    <tr> <td colspan=2> Container uses joblib.Parallel to spread out it's work across batches of files. Diagram depicts the work on each individual file. </td></tr>
    <tr>
        <td width="40%">
            <ol type="1">
                <li>Retrieve the table record using the record ID stored in the workflow manifest.</li>
                <li>Obtain an upload URL from OSDU.</li>
                <li>Move the file to the upload URL using the SAS TOKEN URL for the orignal file followed by uploading the metadata record. </li>
                <li>Retrieve the file version from OSDU to validate it made it.</li>
                <li>Update the associated record in the storage table if succesful.</li>
            </ol>
        </td>
        <td>
            <img width="100%" src="./docs/work_container.jpg"/>
        </td>
    </tr>
</table>

## Storage Table Record

The following describes the storage table records 

|Field|Description|
|-----|-----|
|PartitionKey|Partition in the table for this record.|
|RowKey|A GUID generated so we can record/find specific records.|
|Timestamp|The date/time the record was created.|
|processed|A boolean flag indicating if this record has been processed succesfully. When true, any further searches to upload will ignore this record to avoid duplicates.|
|processed_time|Date time the record was processed succesfully or not.|
|code|The status code ONLY if the processing of the record failed.|
|container_id|The ACI container ID (auto generated guid) to track which container processed the record.|
|file_name|The path in the source file share where the file resides.|
|file_size|Size of the file, in bytes, as it sits in the source file share. Needed because the OSDU SAS URL does not have enough rights to query properties, so this field is used to determine wait times when uploading to OSDU.|
|metadata|The path to the generated metadata in the destination storage account used for logging, etc.|
|source_sas|The SAS URI of the file in the source storage account, these SAS tokens are valid for 24 hours.|
|meta_id|When succesfully processed, this is the OSDU identifier of the metadata record.|

# Next Steps

While there is a lot of work already put into the engine to run/track a generic dataset, there is a lot more that needs to be done to bring this solution to life. 

However, as the next data set is not yet available, the code in this package is in a good state to work on developing a full solution around the new data requirements. 

In it's current form:

- A new dataset that has not been explored may result in customization along the way. This may be for specific file types, or content. 
    - Using the setting DATA_SOURCE_MAP in the __custinput.sh__ file, you can target a specific folder and file type for testing. Will make iterating on different types easy in the pre-container configuration. 
- Can perform a full on functional test of scanning storage, creating neccesary assets, and moving records to an OSDU instance. 

What's not there is the containerization of the functionality. This can be a final stage once understanding the new data set is complete and any customizations have been integrated to the actions within the flow. 

However, containerization is going to be crucial due to the fact that the workload functionality needs to be spread across multiple instances to acheive a throughput that would be acceptabe. 

- Update custrun.sh to generate the storage account used for logging/storage for the movement process, and pass that along to the load "container". 
    - Might want to make this later rather than sooner to expedite testing so that running the script does NOT make you wait for a storage account to be generated. 
- Obtain the new data set and perform a review.
    - Understand the layout and where important data for upload resides - full directory paths and file extensions. 
    - Understand any extensions that will need some sort of pre-processing work.
        - If any are found, modify the load conainer (scanaction.py) for customized work. 
        - Initial test with a limited data set (configured with DATA_SOURCE_MAP in custinput.sh)

When the above seems pretty solid, the following would be the clean up stages. 

- Containerize the Load functionality. 
    - In custtest.py, all of the settings (environment) needed to succesfully run are noted in the load_container_execute() method.
    - Can this container be deleted afterwards?
- Containerize the Workload functionality.
    - In custtest.py, all of the settings (environment) needed to succesfully run are noted in the workload_container_execute() method.
    - Can this containers be deleted afterwards?
- Determine best practice for passing identity/information to reduce, if possible, the need for a user login to Azure to just running the initial script.
    - If the above is possible, it may also be possible to generate a template spec, but has not been researched thouroughly. 
- If containers cannot be deleted, put together a simple script to run in Cloud Shell. Leaving a bunch of out dated containers in your OSDU deployment RG is a bit of bad form. The script would have inputs of:
    - OSDU Subscription/ Resource Group
    - Workload and Load container name templates
    - Executes:
        - Scan RG for containers meeting the template name
        - Deletes the containers from the RG