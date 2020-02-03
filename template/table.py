from template.page import *
from time import time
from template.config import *
from template.index import *

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
        self.indexer = Index()
        
        self.LID_counter = 0 # Used to increment LIDs
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

    def write_to_basePage(self, record, schema_encoding):
		# Base case: Check if record's RID is unique
        if record.rid in self.page_directory.keys():
            print("Error: Record RID is not unique.\n")
            return
		# Determine if new Pages needed, or recently used Pages are reusable
        if self.last_LID_used == -1 or not self.check_page_space(record.rid):
            self.page_directory[record.rid] = [Page() for _ in range(self.num_columns + INIT_COLS)]
            self.total_num_pages += 1
        else:
            self.page_directory[record.rid] = self.page_directory[self.last_LID_used]

        # Make alias for the current record's set of pages
        cur_record_pages = self.page_directory[record.rid]
        
		# Write to metadata columns
        cur_record_pages[RID_COLUMN].write(record.rid)
        cur_record_pages[TIMESTAMP_COLUMN].write(int(time()))
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding)

        # Write to User Data column(s)
        i = INIT_COLS
        while i < (INIT_COLS + self.num_columns):
            cur_record_pages[i].write(record.columns[i-INIT_COLS])
            i += 1

		# Create & insert new entry into indexer
        self.indexer.insert(record.key, record.rid)
        self.last_LID_used = record.rid

    def write_to_tailPage(self, record, schema_encoding):
		# Base case: Check if record's RID is unique
        if record.rid in self.page_directory.keys():
            print("Error: Record RID is not unique.\n")
            return
		# Determine if new Pages needed, or recently used Pages are reusable
        if self.last_TID_used == -1 or not self.check_page_space(record.rid):
            self.page_directory[record.rid] = [Page() for _ in range(self.num_columns + INIT_COLS)]
            self.total_num_pages += 1
        else: # Recycle recently used tail page
            self.page_directory[record.rid] = self.page_directory[self.last_TID_used]

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
        '''
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
        '''
        try:
            baseID = self.indexer.get_positions(record.key)[0]
        except IndexError:
            print("Key not found")
            raise KeyError
        except:
            # Update indirection column
            byte_pos = baseID % PAGE_CAPACITY * DATA_SIZE
            base_pages = self.page_directory[baseID]
            prev_tid = base_pages[INDIRECTION_COLUMN].data[byte_pos:byte_pos + DATA_SIZE]
            schema_data = base_pages[SCHEMA_ENCODING_COLUMN].data
            base_entry = int.from_bytes(schema_data[byte_pos:byte_pos + DATA_SIZE], byteorder="little")
            cur_record_pages[INDIRECTION_COLUMN].write(prev_tid if base_entry else baseID)
            base_pages[INDIRECTION_COLUMN].write(record.rid, byte_pos)

		# Write to rest of metadata columns
        cur_record_pages[RID_COLUMN].write(record.rid)
        cur_record_pages[TIMESTAMP_COLUMN].write(int(time()))
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding)
        
        new_key = record.columns[self.key_index]
        if new_key is not None:
            self.indexer.unique_update(record.key, new_key)
        self.last_TID_used = record.rid

    def get_latest(self, baseIDs):
        rid_output = []
        if isinstance(baseIDs, int):
            baseIDs = list(baseIDs)

        """
        return [int.from_bytes(self.page_directory[baseID][INDIRECTION_COLUMN].data
                                [(baseID % PAGE_CAPACITY * DATA_SIZE):(baseID % PAGE_CAPACITY * DATA_SIZE)+DATA_SIZE],
                               'little'
                               )
                if (int.from_bytes(self.page_directory[baseID][INDIRECTION_COLUMN].data
                                    [(baseID % PAGE_CAPACITY * DATA_SIZE):(baseID % PAGE_CAPACITY * DATA_SIZE)+DATA_SIZE],
                                    'little'
                                    ) != 0) else baseID
                for baseID in baseIDs]
        """
        for baseID in baseIDs:
            # we want to get the value in the indirection column
            byte_pos = baseID % PAGE_CAPACITY * DATA_SIZE
            TID = int.from_bytes(self.page_directory[baseID][INDIRECTION_COLUMN].data[byte_pos:byte_pos+DATA_SIZE], 'little')
            if TID == 0:
                rid_output.append(baseID)
            else:
                rid_output.append(TID)
        
        print("baseIDs in get latest = ", baseIDs)
        return rid_output


    def read_records(self, key, query_columns):
        records = self.indexer.get_positions(key)
        latest_records = self.get_latest(records)
        return [Record(rid, key, [int.from_bytes(enumerated_page[1].data
                                                 [rid % PAGE_CAPACITY * DATA_SIZE:rid % PAGE_CAPACITY * DATA_SIZE + 8],
                                                 'little'
                                                 )
                                  if bool(query_columns[enumerated_page[0]]) else None
                                  for enumerated_page in enumerate(self.page_directory[rid][INIT_COLS:])])
                for rid in latest_records]
        '''
        for rid in latest_records:
            data = []
            for enumerated_page in enumerate(self.page_directory[rid][INIT_COLS:])
                byte_pos = rid % PAGE_CAPACITY * DATA_SIZE
                if bool(query_columns[enumerated_page[0]]):
                    data.append(int.from_bytes(enumerated_page[1][byte_pos:byte_pos + 8], 'little'))
                else:
                    data.append(None)
            output.append(Record(rid, key, data))   
        '''

    def collect_values(self, start_range, end_range, col_index):
        total = 0
        byte_pos = 0
        page_data = None

        rid_list = []
        latest_rids = []

        for rid in range(start_range, end_range):
            if rid in self.page_directory.keys():
                rid_list.append(rid)

        latest_rids = self.get_latest(rid_list)

        for rid in latest_rids:
            byte_pos = rid % PAGE_CAPACITY * DATA_SIZE
            page_data = self.page_directory[rid][INIT_COLS + col_index].data
            col_val = int.from_bytes(page_data[byte_pos:byte_pos + DATA_SIZE], 'little')
            total += col_val

        return total

# previous idea for collect_values below
"""
                # Get latest RID
                indirect_page_data = self.table.page_directory[rid][INDIRECTION_COLUMN].data
                indirect_val = int.from_bytes(indirect_page_data[byte_pos:byte_pos + DATA_SIZE], 'little')
                # Set default values
                byte_pos = rid % PAGE_CAPACITY * DATA_SIZE
                page_data = self.table.page_directory[rid][INIT_COLS + col_index].data
                # Check if any updates were made
                if indirect_val != 0:
                    byte_pos = indirect_val % PAGE_CAPACITY * DATA_SIZE
                    page_data = self.table.page_directory[indirect_val][INIT_COLS + col_index].data
				# Accumulate specified column values
                col_val = int.from_bytes(page_data[byte_pos:byte_pos + DATA_SIZE], 'little')
                total += col_val
"""

