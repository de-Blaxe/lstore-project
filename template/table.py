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
    :param key_index: int       #Index of table.key_index in columns
    """
    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns
        self.page_directory = dict()
        self.record_directory = dict()
        
        self.LID_counter = -1 # Used to increment LIDs
        self.TID_counter = 2**64 - 1 # Used to decrement TIDs 

        # Keep track of available base and tail pages
        self.last_LID_used = -1
        self.last_TID_used = -1

    def __merge(self):
        pass

    def check_page_space(self, rid = None, DATA_SIZE = 8):
        '''
        for column_pages in self.page_directory:
            if column_pages[self.last_LID_used].has_capacity(DATA_SIZE) is False:
                return False
        '''
        return rid % PAGE_CAPACITY != 0

    def modify_record_dir(self, record):
        self.record_directory[record.rid] = record

    def write_to_basePage(self, record, schema_encoding):
        if self.last_LID_used == -1 or not self.check_page_space(record.rid):
            self.page_directory[record.rid] = [Page() for _ in range(self.num_columns + INIT_COLS)]
            self.total_num_pages += 1
        else:
            self.page_directory[record.rid] = self.page_directory[self.last_LID_used]

        # Make alias for the current record's set of pages
        cur_record_pages = self.page_directory[record.rid]
        # Write to RID column
        cur_record_pages[RID_COLUMN].write(record.rid)
        # Write to Timestamp column
        cur_record_pages[TIMESTAMP_COLUMN].write(int(time()))
        # Write to Schema Encoding column
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding)

        # Write to User Data column(s)
        i = INIT_COLS
        while i < (INIT_COLS + self.num_columns):
            cur_record_pages[i].write(record.columns[i-INIT_COLS])
            i += 1

        self.last_LID_used = record.rid

    def write_to_tailPage(self, record, schema_encoding):
        if self.last_TID_used == -1 or not self.check_page_space(record.rid):
            self.page_directory[record.rid] = [Page() for _ in range(self.num_columns + INIT_COLS)]
            self.total_num_pages += 1
        else: # Recycle recently used tail page
            self.page_directory[record.rid] = self.page_directory[self.last_TID_used]

        #TODO: Check if rid exists already in page directory

        # Make alias for the current record's set of pages
        cur_record_pages = self.page_directory[record.rid]
        
        # Update specified columns
        i = INIT_COLS
        while i < (INIT_COLS + self.num_columns):
            if record.columns[i-INIT_COLS] is not None:
                cur_record_pages[i].write(record.columns[i-INIT_COLS])
            else: # Write default values
                cur_record_pages[i].write(0)
            i += 1

        # (Linear) Search thru a subset of basePages for correct basePage
        for baseID in range(self.LID_counter):
            # Make aliases
            byte_pos = baseID % PAGE_CAPACITY * DATA_SIZE
            pages = self.page_directory[baseID]
            page_data = pages[INIT_COLS + self.key_index].data
            base_key = int.from_bytes(page_data[byte_pos:byte_pos+DATA_SIZE], byteorder="little")
            # Matching basePage for record found
            if base_key == record.key:
               page_data = pages[SCHEMA_ENCODING_COLUMN].data
               prev_tid = pages[INDIRECTION_COLUMN].data[byte_pos:byte_pos+DATA_SIZE]
               # Check if record is being updated for 1st time
               #     True:  tail record's indirection => base record's RID
               #     False: tail record's indirection => previous tail record's TID
               base_entry = int.from_bytes(page_data[byte_pos:byte_pos+DATA_SIZE], byteorder="little")
               if not base_entry: # Empty/default entry
                   self.page_directory[record.rid][INDIRECTION_COLUMN].write(baseID, byte_pos)
               else:
                   cur_record_pages[INDIRECTION_COLUMN].write(prev_tid, byte_pos)
               break

        # Write to RID column
        cur_record_pages[RID_COLUMN].write(record.rid)
        # Write to Timestamp column
        cur_record_pages[TIMESTAMP_COLUMN].write(int(time()))
        # Write to Schema encoding column
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding)

        self.last_TID_used = record.rid

# TODO get rid of record_directory and modify_record_dir
# TODO store each record's byte_pos in page directory too
    def read_pages(self, key, query_columns):
        # Base case: Check if a record exists with matching key
        # NOTE: Index.locate() should return RIDs (invalid RIDs: -1)
        # rid_found = self.indexer.locate(key)
        # if rid_found != INVALID_RECORD: 
        #     for itr in len(query_columns):
        #         if query_columns[itr]: # Read current column
        #             page = self.page_directory[rid_found][INIT_COLS + itr]
        #             
        # else:
        # Return [] # empty list, no record exists with given key
        pass
