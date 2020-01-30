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
        self.page_directory = [{}] * (num_columns + INIT_COLS)

        # IDEA: Let the key be a base page number and the values are lists of lists that define its corresponding pages
        # so 5 columns have 5 pages inside base page number 1
        # a new base page with new pages are created after every 512 records
        # This way page dir only needs to map RID to page number and offset
        # Time for inserts = ~0.15 seconds
        self.page_collections = {}
        self.base_page_count = 1
        # Valid RIDs start at 0
        self.RID_counter = -1
        self.last_RID_used = -1

    def __merge(self):
        pass

    def check_page_space(self, rid = None, data_size = 8,):
        '''
        for column_pages in self.page_directory:
            if column_pages[self.last_RID_used].has_capacity(data_size) is False:
                return False
        '''
        return rid % PAGE_CAPACITY != 0

    def write_to_basePage(self, record, schema_encoding):
        # Check for initial case (creation of table) or for capacity of page
        #
        if self.last_RID_used == -1 or not self.check_page_space(record.rid):
            for RID_to_column_page in self.page_directory:
                RID_to_column_page[record.rid] = Page()

        # write to RID column
        self.page_directory[RID_COLUMN][record.rid].write(record.rid)
        # write to Timestamp column
        self.page_directory[TIMESTAMP_COLUMN][record.rid].write(int(time()))
        # write to Schema encoding column
        self.page_directory[SCHEMA_ENCODING_COLUMN][record.rid].write(schema_encoding)
       
        # Create an offset for columns that contain the actual data
        column_offset = INIT_COLS
        for column in record.columns:
            self.page_directory[column_offset][record.rid].write(column)
            column_offset += 1 
