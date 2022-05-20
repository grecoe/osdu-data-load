# Massive Files

# Google Docs

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
Object neded to move file

Object and Container needed on source if moving a folder. 

# Observations
Using cloudshell and SAS URL's to targets, obsservations on moving data with azcopy.

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
- Elapsed Time (Minutes): 3.9409
- Elapsed Time (Minutes): 3.837

## Datasets 
- TNO Data 1.15 GB
- MBPS AVG: 5
- Elapsed Time (Minutes): 7.447
- Number of File Transfers: 44573
- TotalBytesTransferred: 1241924419

**SUPER FAST**
