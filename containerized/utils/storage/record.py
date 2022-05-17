##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import uuid

class Record:
    """
    Storage Table record per file to upload. 
    """
    def __init__(self, partition_key:str):
        self.table_name = None
        self.PartitionKey = partition_key
        self.RowKey = str(uuid.uuid4())
        # Timestamp when the file was processed
        self.processed_time = ""
        # Status code if the processing failed
        self.code = ""
        # Name of the file (path) in file share
        self.file_name = ""
        # Size in bytes of the source file
        self.file_size=0
        # SAS URL to the file in the file share
        self.source_sas = ""
        # Path in the file share to the metadata associated with this file
        self.metadata = ""
        # Container ID that processed the record
        self.container_id = ""
        # The record ID in OSDU
        self.meta_id = ""
        # Indicates if the file has been processed or not.
        self.processed = False

    def get_entity(self):
        """
        Entity is everyting in self.__dict__ EXCEPT the 
        table name. This is used to pass to the storage API for creating
        or updating a table record. 
        """
        entity = {}
        for prop in self.__dict__:
            if prop != 'table_name':
                entity[prop] = self.__dict__[prop]

        return entity

    @staticmethod
    def from_entity(table:str, obj:dict) -> object:
        """
        Create an instance of Record from a dictionary of data retrieved
        from the table storage API
        """

        record = obj
        if isinstance(record,list):
            record = record[0]

        return_obj = Record(None)
        return_obj.table_name = table

        for val in record:
            setattr(return_obj, val, record[val])
        return return_obj