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
    # Retreive record via its key
	# Delete/mark an existing entry in record directory
    # self.table.modify_record_dir(record)
        pass

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        # Create a new Record instance
        schema_encoding = 0 * self.table.num_columns
        self.table.LID_counter += 1 
        baseID = self.table.LID_counter
        record = Record(rid=baseID, key=columns[self.table.key_index], columns=columns) 
        # Write new record to a base page
        self.table.write_to_basePage(record, schema_encoding)
        # Insert new entry to record directory
        self.table.modify_record_dir(record)

        # IDEAS:
        # Error checking: does the key already exist
        # Find spot for data/ generate RID based on spot
        #   case 1: There is space on the last page
        #   case 2: Generate a new page
        #if len(self.table.page_directory) == 0 or  :

        # Generate RID for record
        # Check if all base pages have enough space to fit new insert
        # If not, create new base pages
        # page_row should be the row/offset that the record is in

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        self.table.read_pages(key, query_columns)
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        # Determine schema encoding
        schema_encoding = list(map(lambda i: int(not(i is None)), columns))
        # Create a new Record instance
        tailID = self.table.TID_counter
        record = Record(rid=tailID, key=key, columns=columns) 
        # Write tail record to tail page
        self.table.write_to_tailPage(record, schema_encoding)
        self.table.TID_counter -= 1
        # Update existing entry in record directory
        self.table.modify_record_dir(record)

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
