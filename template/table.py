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
        
        # Valid RIDs start at 0
        self.RID_counter = -1 # TODO: Change name to BIDs (start from 0)
        self.TID_counter = 2**64 - 1

        self.last_RID_used = -1
        self.last_TID_used = -1

    def __merge(self):
        pass

    def check_page_space(self, rid = None, data_size = 8):
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

        # Make alias
        cur_record_pages = self.page_directory[record.rid]
        # Write to RID column
        cur_record_pages[RID_COLUMN].write(record.rid)
        # Write to Timestamp column
        cur_record_pages[TIMESTAMP_COLUMN].write(int(time()))
        # Write to Schema encoding column
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding)

        i = INIT_COLS
        while i < (INIT_COLS + self.num_columns):
            cur_record_pages[i].write(record.columns[i-INIT_COLS])
            i += 1

        self.last_RID_used = record.rid

    def write_to_tailPage(self, record, schema_encoding):
        if self.last_TID_used == -1 or not self.check_page_space(record.rid):
            self.page_directory[record.rid] = [Page() for _ in range(self.num_columns + INIT_COLS)]
        else:
            self.page_directory[record.rid] = self.page_directory[self.last_TID_used]

        # Make alias
        cur_record_pages = self.page_directory[record.rid]
        
        # Update specified columns
        i = INIT_COLS
        while i < (INIT_COLS + self.num_columns):
            if record.columns[i-INIT_COLS] is not None:
                cur_record_pages[i].write(record.columns[i-INIT_COLS])
            else: # Insert default values
                cur_record_pages[i].write(0)
            i += 1

        # write to Indirection column
        # linear search a subset of pages for matching basePage
        base_indirection = 0
        for baseID in range(self.RID_counter):
            byte_pos = baseID % PAGE_CAPACITY * data_size
            pages = self.page_directory[baseID]
            page_data = pages[self.key].data         
            target = int.from_bytes(page_data[byte_pos:byte_pos+data_size], byteorder="little")
            if target == record.key:
               # update indirection column for base record
               prev_tid = pages[INDIRECTION_COLUMN].data[byte_pos:byte_pos+data_size]
               # check if it's the first update to a record
               # a) if true: tail record's indirection => base record's RID & 
               page_data = pages[SCHEMA_ENCODING_COLUMN].data
               if not int.from_bytes(page_data[byte_pos:byte_pos+data_size], byteorder="little"):
                   self.page_directory[record.rid][INDIRECTION_COLUMN].write(baseID, byte_pos)
               # b) else: "" => previous tail record's TID (not the first time record has been updated)
               else:
                   cur_record_pages[INDIRECTION_COLUMN].write(prev_tid, byte_pos)
               break

        # write to RID column
        cur_record_pages[RID_COLUMN].write(record.rid)
        # write to Timestamp column
        cur_record_pages[TIMESTAMP_COLUMN].write(int(time()))
        # write to Schema encoding column
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding)

        self.last_TID_used = record.rid
