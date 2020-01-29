from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


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
        self.page_collections = [{}]
        # Valid RIDs start at 1
        self.RID_counter = 0
        self.last_RID_used = 0
        pass

    def __merge(self):
        pass

    def check_page_space(self, data_size = 8):
        for page_collection in self.page_collections:
            page_collection[self.last_RID_used]