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
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        # Create a new Record instance
        schema_encoding = [0] * self.table.num_columns
        self.table.LID_counter += 1 
        baseID = self.table.LID_counter
        record = Record(rid=baseID, key=columns[self.table.key_index], columns=columns) 
        # Write new record to a base page
        self.table.write_to_basePage(record, schema_encoding)

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        return self.table.read_records(key, query_columns)


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
        self.table.write_to_tailPage(record, schema_encoding)
        self.table.TID_counter -= 1

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # Increment start_range by 1 until end_range
    :param col_index: int  			# Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, col_index):
        return self.table.collect_values(start_range, end_range, col_index)
