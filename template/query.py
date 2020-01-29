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
        schema_encoding = '0' * self.table.num_columns
        # Error checking: does the key already exist
        # Find spot for data/ generate RID based on spot
        #   case 1: There is space on the last page
        #   case 2: Generate a new page
        if len(self.table.page_directory) == 0 or  :

        # Generate RID for record
        # Check if all base pages have enough space to fit new insert
        # If not, create new base pages
        # page_row should be the row/offset that the record is in

        pass

    """
    # Read a record with specified key
    """

    def select(self, key, query_columns):
        pass

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        pass
