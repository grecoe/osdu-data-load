# Large ingest

Collection for thoughts/approaches to move 10-100GB files into or around Azure.

Collected some interesting numbers in [massive_files.md](./massive_files.md) to review. 

Standard environment to run the python here (for now), but was run in bash. Will also work with cmd, I believe.


# azcopy
[azcopy](https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10) works from storage (blob/file), S3, and stack storage. 

Don't think it works from data lake or other, but have to look into it. 

# Storage Services

- [Azure Storage](https://docs.microsoft.com/en-us/azure/storage/common/scalability-targets-standard-account)

    - Indicates limit at 5PiB per account?? Much lower on file shares, so blob likely the solution

- [Azure File Shares](https://docs.microsoft.com/en-us/azure/storage/files/storage-files-scale-targets)

    - 5TB standard (v2)
    - 100TB Premium