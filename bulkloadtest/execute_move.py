from utils.configuration import Configuration, Storage
from utils.storage import StorageUtil
from utils.copyutil import StorageCopy
from datetime import datetime, timedelta
import json
import typing
import time
from joblib import Parallel, delayed

config:Configuration = Configuration("./config.ini")

def _batch(items:list, batch_size:int) -> typing.List[str]:
    """list is generic because it uses different types, batches up 
    a list based on size of batch requested and returns a sub list
    with that many items in it until it is exhausted"""
    idx = 0
    while idx < len(items):
        yield items[idx: idx + batch_size]
        idx += batch_size

# Get Source Sas
if not config.source.account_sas:
    StorageUtil.get_account_sas(
        config.source,
        read=True,
        write=True,
        create=True,
        add=True,
        list_content=True
    )

#config.source.account_sas = "sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacupitfx&se=2022-05-23T01:49:38Z&st=2022-05-22T17:49:38Z&spr=https&sig=XyZgiiUp2KTy3znJP3ijzvmplp4UURhHRDNrfYOmaoc%3D"

if not config.destination.account_sas:
    StorageUtil.get_account_sas(
        config.destination,
        read=True,
        write=True,
        list_content=True,
        create=True,
        add=True
    )

#config.destination.account_sas = "sv=2020-08-04&ss=bfqt&srt=co&sp=rwdlacupitfx&se=2022-05-23T01:47:38Z&st=2022-05-22T17:47:38Z&spr=https&sig=LEjjs3%2BYJiWCXa2d0gJvk1GfkaTLNkEO3Sp5tnNquHk%3D"


files = StorageUtil.get_files(config.source)
targets = []
for file in files:
    location = StorageUtil.get_file_url(config.destination, file[0])
    print("Captured file", file[0])
    targets.append((file[1], location))



    #StorageCopy.copy_storage_file(config.az_copy_location, file[1], location)
    #quit()
print("Starting ", datetime.now())
start = datetime.utcnow()

process_results = []
for batch in _batch(targets, 2):
    #for target in targets:
    try:
        process_results += Parallel(n_jobs=len(targets), timeout=600.0)(delayed(StorageCopy.copy_storage_file)(config.az_copy_location, target[0], target[1]) for target in batch)
        break
    except Exception as ex:
        print("Generic Exception")
        print(type(ex))

end = datetime.utcnow()

delta = end - start
print("Total Run Time: ", datetime.now(), " = ",  delta.total_seconds()/60 )
for r in process_results:
    print(r.__dict__)

print("Done")
