##########################################################
# Copyright (c) Microsoft Corporation.
##########################################################
import typing
import datetime
from utils.storage.record import Record
from azure.data.tables import TableServiceClient, TableClient, UpdateMode
from azure.data.tables._entity import EntityProperty
from azure.data.tables._deserialize import TablesEntityDatetime

class AzureTableStoreUtil:
    """
    Class encapsulating the calls to an Azure Storage Table 
    """

    CONN_STR = "DefaultEndpointsProtocol=https;AccountName={};AccountKey={};EndpointSuffix=core.windows.net"

    def __init__(self, account_name:str, account_key:str):
        self.connection_string = AzureTableStoreUtil.CONN_STR.format(
            account_name,
            account_key
        )

    def search_unprocessed(self, table_name:str) -> typing.List[Record]:
        """
        Search the table for all records that are not processed yet. This will help
        if we ever need to re-run a container to retry failed records. 
        Params:
        table_name - required: Yes  Storage Table to search

        Returns:
        List of Record objects for each record that has not been processed
        """
        return_records = []
        with self._create_table(table_name) as log_table:
            with self._get_table_client(table_name) as table_client:
                query_filter = AzureTableStoreUtil._get_query_filter_unprocessed()

                raw_records = self._parse_query_results(table_client, query_filter)
                for raw in raw_records:
                    return_records.append(Record.from_entity(table_name, raw))

        return return_records

    def search_table_id(self, table_name:str, recordid:str) -> typing.List[Record]:
        """
        Search the table for a specific record (RowKey). 

        Params:
        table_name - required: Yes  Storage Table to search
        recordid   - required: Yes  RowKey of the record to find. 

        Returns:
        List of Record objects for each record that has not been processed
        """
        return_records = []
        with self._get_table_client(table_name) as table_client:
            query_filter = AzureTableStoreUtil._get_query_filter_id(recordid)
            raw_records = self._parse_query_results(table_client, query_filter)
            for raw in raw_records:
                return_records.append(Record.from_entity(table_name, raw))

        return return_records

    def search_table_filename(self, table_name:str, file_name:str) -> typing.List[Record]:
        """
        Search the table for a specific record by file name, this is the whole
        path of where the file sat in the source file share

        Params:
        table_name - required: Yes  Storage Table to search
        file_name  - required: Yes  File path to search for. 

        Returns:
        List of Record objects for each record that has not been processed
        """
        return_records = []
        with self._get_table_client(table_name) as table_client:
            query_filter = AzureTableStoreUtil._get_query_filter_name(file_name)
            raw_records = self._parse_query_results(table_client, query_filter)
            for raw in raw_records:
                return_records.append(Record.from_entity(table_name, raw))

        return return_records

    def update_record(self, table_name:str, entity:Record) -> None:
        """
        Update a record in the storage table. Creates the table if not already
        present.

        Params:
        table_name - required: Yes  Storage Table to search
        entity     - required: Yes  Record to update 

        Returns:
        """
        with self._get_table_client(table_name) as table_client:
            table_client.upsert_entity(mode=UpdateMode.REPLACE, entity=entity.get_entity())

    def delete_record(self, table_name:str, row_key:str, partition:str) -> None:
        """
        Delete a record from the storage table. 

        Params:
        table_name - required: Yes  Storage Table to search
        row_key    - required: Yes  RowKey of the record to delete
        partition  - required: Yes  Partition ID to use

        Returns:
        """
        self.delete_records(table_name, [(row_key, partition)])

    def delete_records(self, table_name:str, records:typing.List[typing.Tuple[str,str]]) -> None:
        """
        Delete records from a table
        
        Parameters:
        table_name - name of table to remove. 
        records - List of tuples that are (RowKey,PartitionKey)
        """
        with self._get_table_client(table_name) as table_client:
            for pair in records:
                table_client.delete_entity(
                    row_key=pair[0], 
                    partition_key=pair[1]
                    )

    def add_record(self, table_name:str, entity:Record):
        """
        Add a record to a table

        Parameters:
        table_name - Name of table to add to
        entity - Dictionary of non list/dict data
        """
        with self._create_table(table_name) as log_table:
            try:
                entity.table_name = table_name
                resp = log_table.create_entity(entity=entity.get_entity())
            
            except ConnectionResetError as ex:
                # Saw this in testing...we should definitley retry it.
                try:
                    resp = log_table.create_entity(entity=entity.get_entity())
                except Exception as ex:
                    print("Entity create failed retry- {}".format(entity.file_name))
                    print(str(ex))

            except Exception as ex:
                print("Entity create failed - {}".format(entity.file_name))
                print(str(ex))

    @staticmethod
    def _get_query_filter_unprocessed() -> str:
        """
        Build the query string to get all unprocessed records
        """
        query_filter = "processed eq false"
        return query_filter

    @staticmethod
    def _get_query_filter_name(file_name:str) -> str:
        """
        Build the query string to get a record by filename
        """
        query_filter = "file_name eq '{}'".format(
            file_name,
        )

        return query_filter

    @staticmethod
    def _get_query_filter_id(rowkey:str) -> str:
        """
        Build the query string to get a record by it's row key
        """
        query_filter = "RowKey eq '{}'".format(
            rowkey,
        )

        return query_filter

    def _parse_query_results(self, table_client:TableClient, query:str) -> typing.List[dict]:
        """
        Query the storage table with a given query and return the results as a list of 
        dictionaries. 

        Parameters:

        table_client:
            Client to perform the query on
        query:
            String query to execute
        """
        return_records = []

        results = table_client.query_entities(query)
        if results:
            for result in results:
                entity_record = {}
            
                for key in result:
                    value = result[key]

                    if isinstance(result[key], EntityProperty): 
                        value = result[key].value
                    if isinstance(result[key], TablesEntityDatetime):
                        value = datetime.datetime.fromisoformat(str(result[key]))

                    entity_record[key] = value
                
                return_records.append(entity_record)
        else:
            message = "Failed to get results for query: {}".format(query)
            print(message)

        return return_records

    def _create_table(self, table_name:str) -> TableClient:
        """
        Ensure a table exists in the table storage 
        """
        return_client = None
        try:
            return_client = self._get_table_client(table_name)
        except Exception as ex:
            pass


        if not return_client:
            with TableClient.from_connection_string(conn_str=self.connection_string, table_name=table_name) as table_client:
                try:
                    table_client.create_table()
                except Exception as ex:
                    pass
            
            return_client = self._get_table_client(table_name) 

        return return_client

    def _get_table_client(self, table_name: str) ->TableClient:
        """
        Searches for and returns a table client for the specified
        table in this account. If not found throws an exception.
        """
        return_client = None

        with TableServiceClient.from_connection_string(conn_str=self.connection_string) as table_service:
            name_filter = "TableName eq '{}'".format(table_name)
            queried_tables = table_service.query_tables(name_filter)

            found_tables = []
            for table in queried_tables:
                # Have to do this as its an Item_Paged object
                if table.name == table_name:
                    found_tables.append(table)
                    break 
        
            if found_tables and len(found_tables) == 1:
                return_client = TableClient.from_connection_string(conn_str=self.connection_string, table_name=table_name)
            else:
                raise Exception("Table {} not found".format(table_name))

        return return_client                