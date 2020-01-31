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
        self.page_directory = dict()

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
        if self.last_RID_used == -1 or not self.check_page_space(record.rid):
            self.page_directory[record.rid] = [Page() for _ in range(self.num_columns + INIT_COLS)]
        else:
            self.page_directory[record.rid] = self.page_directory[self.last_RID_used]

        cur_record_pages = self.page_directory[record.rid]
        # write to RID column
        cur_record_pages[1].write(record.rid)
        # write to Timestamp column
        cur_record_pages[2].write(int(time()))
        # write to Schema encoding column
        cur_record_pages[3].write(schema_encoding)

        i = INIT_COLS
        while i < (INIT_COLS + self.num_columns):
            cur_record_pages[i].write(record.columns[i-INIT_COLS])
            i += 1

        self.last_RID_used = record.rid
