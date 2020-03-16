from template.table import Table, Record
from template.index import Index
from template.config import *

import copy

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False 
    """
    def __init__(self, table):
        self.table = table
        pass


    """
    # Internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """
    def delete(self, key):
        self.table.delete_record(key) # TODO need to return False or True
        pass


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """
    def insert(self, *columns):
        # Create a new Record instance
        schema_encoding = '0' * self.table.num_columns
        self.table.LID_counter += 1 
        baseID = self.table.LID_counter
        record = Record(rid=baseID, key=columns[self.table.key], columns=columns) 
        # Write new record to a base page
        self.table.insert_baseRecord(record, schema_encoding)


    """
    # Read a record with specified key
    # :param key: the key value to select records based on
    # :param query_columns: what columns to return. Array of 1 and/or 0s.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
    def select(self, key, column, query_columns):
        # Read record for given key and column
        record = self.table.read_records(key, column, query_columns)
        return record


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key 
    # or if the target record cannot be accessed due to 2PL locking
    """
    def update(self, key, *columns):
        # Determine schema encoding
        schema_encoding = list(map(lambda i: int(not(i is None)), columns))
        # Create a new Record instance
        assert key is not None
        # Atomically update TID counter
        self.table.TID_counter_lock.acquire()
        unique_rid = copy.copy(self.table.TID_counter)
        record = Record(rid=unique_rid, key=key, columns=columns)
        self.table.TID_counter -= 1
        self.table.TID_counter_lock.release()
        # Write tail record to tail page
        return self.table.insert_tailRecord(record, schema_encoding)



    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # Increment start_range by 1 until end_range
    :param col_index: int  	        # Index of desired column to aggregate

    # This function is only called on the primary key
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """
    def sum(self, start_range, end_range, col_index):
        return self.table.collect_values(start_range, end_range, col_index)


    """
    # Increments one column of the record
    # This implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        # originally, r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1 # r[column] + 1 
            # TODO: NEED TO RENAME TABLE.KEY_INDEX TO TABLE.KEY 
            u = self.update(key, *updated_columns)
            return u 
        return False
