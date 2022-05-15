import uuid

class Record:
    """
    Storage Table record per file to upload. 
    """
    def __init__(self, partition_key:str):
        self.table_name = None
        self.PartitionKey = partition_key
        self.RowKey = str(uuid.uuid1())
        self.processed_time = ""
        self.file_name = ""
        self.file_size=0
        self.source_sas = ""
        self.metadata = ""
        self.container_id = ""
        self.meta_id = ""
        self.processed = False

    def get_entity(self):
        """
        Entity is everyting in self.__dict__ EXCEPT the 
        table name.
        """
        entity = {}
        for prop in self.__dict__:
            if prop != 'table_name':
                entity[prop] = self.__dict__[prop]

        return entity

    @staticmethod
    def from_entity(table:str, obj:dict) -> object:

        record = obj
        if isinstance(record,list):
            record = record[0]

        return_obj = Record(None)
        return_obj.table_name = table

        for val in record:
            setattr(return_obj, val, record[val])
        return return_obj