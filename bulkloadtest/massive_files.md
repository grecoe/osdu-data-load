# azcopy - Massive Files

There comes a need when a customer needs to move in a massive dataset to Azure for storage/processing. This repo folder contains some information and a process of using azcopy to see what kinds of throughput can be acheived. 

Using standard HTTP(s) calls to move data is not sustainable because the process takes far too long. For example, setting up a dowload from Google Docs to a folder which was an Azure File Share for a 101GB file took > 7 hours. 

Moving the same file internal to Azure (storage 1 -> storage 2) can be moved in approximately 3.8 minutes. 

However, this would still require 2 things:
- Original data set exists in Azure (Databox?)
- It still takes approximately 27 days to move 1PB within Azure with the azcopy utility.

<b>Content</b>
- [Source Data](#source---google-docs)
- [Moving Data](#moving-them-around)
    - [SAS Tokens](#sas)
    - [Applications](#applications)
- [Observations](#observations)
    - [Single Files](#single-files)
        - Single large files
    - [Datasets](#datasets)
        - Multiple smaller files
    - [Batch - Python](#batch-with-parallel-from-one-machine)
        - Batch large files with python
    - [Pseudo Batch - Cloud Shell](#manual-batch-cloud-shell)
        - Batch large files with Cloudshell
    - [Batch - Cloud Shell](#cloudshell-batch)
        - Move entire directory with 5 - 101 GB files

# Source - Google Docs

Load files from [google docs](https://wiki.seg.org/wiki/Open_data#Poseidon_3D_seismic.2C_Australia) by creating a file share, mount it to laptop, then change download location to the share. 

# Moving them around

- Use CloudShell azcopy
- Create SAS on account with source file share then build URL:
    - https://[ACCOUNT].file.core.windows.net/[SHARE]/[PATH]/[FILE].zgy?[ACCOUNT_SAS]
- Create a SAS on the desitnation share
    - https://[ACCOUNT].file.core.windows.net/[SHARE]/[PATH]/[FILE].zgy?[ACCOUNT_SAS]
    - You can change file name to make multiple copies
    - You can also NOT provide a name and it will use the name of the source file. 
- Run az copy
    - azcopy cp "SOURCE" "DESTINATION"

## SAS

This source has the ability to generate SAS tokens, but they don't seem to be taken in by azcopy. I resorted to generating SAS tokens using the Azure Portal and placing them into the ini file to process. 

- Object neded to move file
- Object and Container needed on source if moving a folder. 

## Applications
- azcopytest.py
    - Provide information in this one file to move one file
- execute_move.py
    - Add settings to config.ini for source folder and destination folder

# Observations
Using cloudshell and SAS URL's to targets, obsservations on moving data with azcopy. Then batched it in Python, then finally two versions of Cloud Shell moving two different files. 

When cloudshell works on a single file, it's incredibly fast. When it's batched (more than one file at a time in different processes pushing it) it seems to act almost synchronously. That is, the time goes up linearly with more copies fired at the same time. 

However, this approach seems to be able to handle 

- 1 101 GB file in ~ 3.75 minutes (3:45)
- Since it seems parallel this translates to, a constant running copy process
    - 1440/3.75 = 384 files = 38,784 GB == ~37.87 TB / Day
    - ~37.87 TB / Day -> 1024 / 37.87 -> 1PB == 27 days


## Single Files
- 15.72 GB
- MBPS AVG 5
- TotalBytesTransferred: 16879190016
- Number of File Transfers: 1
- Elapsed Time (Minutes): 0.7007
- Elapsed Time (Minutes): 0.8069
- Elapsed Time (Minutes): 0.6428

100.99GB
- MBPS AVG > 3300 (3000-6500)
- TotalBytesTransferred: 108440556884Elapsed - Number of File Transfers: 1
- Elapsed Time (Minutes): 4.2702
    - 23.65 GB/Min
- Elapsed Time (Minutes): 3.9409
    - 25.63 GB/Min
- Elapsed Time (Minutes): 3.837
    - 26.32 GB/Min

## Datasets 
- TNO Data 1.15 GB
- MBPS AVG: 5
- Elapsed Time (Minutes): 7.447
- Number of File Transfers: 44573
- TotalBytesTransferred: 1241924419


## Batch with parallel from one machine
2 101GB Files - Per File
- {'success': True, 'minutes': 6.5692}
- {'success': True, 'minutes': 6.5358}
    - 30.75 GB/Min
2 101GB Files - Per File
- {'success': True, 'minutes': 7.7696}
- {'success': True, 'minutes': 5.7022}
    - 26 GB / Min
1 101GB Files - Per File
- {'success': True, 'minutes': 3.6376}
    - 27.77 GB/Min

## Manual Batch (Cloud Shell) 
Fire two copies (different 101GB files) from 2 different cloud shell instances.

- Elapsed Time (Minutes): 7.9368
- Elapsed Time (Minutes): 5.2767

## Cloudshell batch
Copy the whole directory of 5 101GB files

- Elapsed Time (Minutes): 20.3089
- Number of File Transfers: 5
- Number of Folder Property Transfers: 1
- TotalBytesTransferred: 542202784420 (504.96 GB)
- 24.86 GB/Min (so very close to those marked above)


<b>Why this won't work for OSDU</b>

When adding files to OSDU, you first have to get an upload URL. This is a Signed (SAS) URL to the location to put the file. 

In this scenario, we are just blindly dropping files into another Azure Storage Account without regard to an actual location. 

So, the reality is, each file needs to be processed individually (after getting the upload url).
