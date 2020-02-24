from template.table import Table, Record
from template.index import Index
from template.config import *

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """
    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """
    def delete(self, key):
        self.table.delete_record(key)
        pass


    """
    # Insert a record with specified columns
    """
    def insert(self, *columns):
        # Create a new Record instance
        schema_encoding = '0' * self.table.num_columns # Changed template code (array -> string)
        self.table.LID_counter += 1 
        baseID = self.table.LID_counter
        record = Record(rid=baseID, key=columns[self.table.key_index], columns=columns) 
        # Write new record to a base page
        self.table.insert_baseRecord(record, schema_encoding)


    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    """
    def select(self, key, column, query_columns):
        # Create index for other columns only if needed
        if column is not self.table.key_index:
            self.table.indexer.create_index(column)
            # We need to now possibly read records from multiple keys -- so we need to use these keys as the parameter for read_records
            record = self.table.read_records(self.table.indexer.indices[column], column, query_columns)
            # Drop index only for non-primary column indexes
            self.table.indexer.drop_index(self.table, column)
        else:
            record = self.table.read_records(key, column, query_columns)
        
        return record


    """
    # Update a record with specified key and columns
    """
    def update(self, key, *columns):
        # Determine schema encoding
        schema_encoding = list(map(lambda i: int(not(i is None)), columns))
        # Create a new Record instance
        assert key is not None
        record = Record(rid=self.table.TID_counter, key=key, columns=columns) 
        # Write tail record to tail page
        self.table.insert_tailRecord(record, schema_encoding)
        self.table.TID_counter -= 1

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # Increment start_range by 1 until end_range
    :param col_index: int  	        # Index of desired column to aggregate
    # This function is only called on the primary key
    """
    def sum(self, start_range, end_range, col_index):
        return self.table.collect_values(start_range, end_range, col_index)
