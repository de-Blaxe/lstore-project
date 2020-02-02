from template.table import Table
from template.config import *
import bisect
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self, table):
        '''


        '''
        index = [(int.from_bytes(self.page_directory[baseID][INIT_COLS + self.key_index].data[baseID % PAGE_CAPACITY * DATA_SIZE:baseID % PAGE_CAPACITY * DATA_SIZE + DATA_SIZE], "little"), baseID) for baseID in range(table.LID_counter)]
        tuple_first = lambda x: x[0]
        self.index = sorted(index, tuple_first)
        '''
        for baseID in range(table.LID_counter):
            byte_pos = baseID % PAGE_CAPACITY * DATA_SIZE
            pages = self.page_directory[baseID]
            page_data = pages[INIT_COLS + self.key_index].data
            base_key = int.from_bytes(self.page_directory[baseID][INIT_COLS + self.key_index].data[baseID % PAGE_CAPACITY * DATA_SIZE:baseID % PAGE_CAPACITY * DATA_SIZE + DATA_SIZE], "little")
        '''

        pass

    """
    # returns the location of all records with the given value
    """

    def locate(self, value):

        tuple_first = lambda x: x[0]
        try:
            key_index = map(tuple_first, self.index)
            bisect.bisect
        except ValueError:
            return []

        return self.rid_list[index]

        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, table, column_number):
        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, table, column_number):
        pass
