from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
INIT_COLS = 4
PAGE_CAPACITY = 512

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns


class Table:
    total_num_pages = 0
    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}

        # IDEA: Let the key be a base page number and the values are lists of lists that define its corresponding pages
        # so 5 columns have 5 pages inside base page number 1
        # a new base page with new pages are created after every 512 records
        # This way page dir only needs to map RID to page number and offset
        # Time for inserts = ~0.15 seconds
        self.page_collections = {}
        self.base_page_count = 1
        # Valid RIDs start at 1
        self.RID_counter = 0

    def __merge(self):
        pass

    def check_page_space(self, data_size = 8):
        for page_collection in self.page_collections:
            page_collection[self.last_RID_used]

    def write_to_basePage(self, record, schema_encoding):
        # Check for initial case (creation of table) or for capacity of page
        if len(self.page_collections) == 0 or record.rid % PAGE_CAPACITY == 0:
            self.page_collections[self.base_page_count] = [[] for _ in range(self.num_columns + INIT_COLS)]
            self.base_page_count += 1
       
        # Check if indirection column exists
        # If the condition is true it indicates that none of the columns have values in them so we can initialize all metadata
        if not self.page_collections[self.base_page_count-1][INDIRECTION_COLUMN]:
            # indirection column is set to NULL for inserts
            self.page_collections[self.base_page_count-1][INDIRECTION_COLUMN] = Page()
            # RID column is initialized
            self.page_collections[self.base_page_count-1][RID_COLUMN] = Page()
            # Timestamp column is initialized
            self.page_collections[self.base_page_count-1][TIMESTAMP_COLUMN] = Page()
            # Schema encoding column is initialized
            self.page_collections[self.base_page_count-1][SCHEMA_ENCODING_COLUMN] = Page() 
        
        # write to RID column
        self.page_collections[self.base_page_count-1][RID_COLUMN].write(record.rid)
        # write to Timestamp column
        self.page_collections[self.base_page_count-1][TIMESTAMP_COLUMN].write(int(time()))
        # write to Schema encoding column
        self.page_collections[self.base_page_count-1][SCHEMA_ENCODING_COLUMN].write(schema_encoding)
       
        # Create an offset for columns that contain the actual data
        column_offset = INIT_COLS
        for column in record.columns:
            # Check if the column exists, if it doesnt, create a page, else just insert
            if not self.page_collections[self.base_page_count-1][column_offset]:
                self.page_collections[self.base_page_count-1][column_offset] = Page()
            
            # write to page
            self.page_collections[self.base_page_count-1][column_offset].write(column)
            column_offset += 1 
