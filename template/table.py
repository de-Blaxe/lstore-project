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
        # Determine capacity, based on type of RID given
        if rid >= self.TID_counter: # Tail recordID
            return abs(rid - (2**64 - 1)) % PAGE_CAPACITY
        else: # Base recordID
            return (rid-1) % PAGE_CAPACITY != 0

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
        schema_encoding_string = ''
        for bit in schema_encoding:
            schema_encoding_string += str(bit)
        schema_encoding_int = int(schema_encoding_string)
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding_int)

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
            index_position = self.indexer.get_positions(record.key)[0]
            baseID = self.indexer.index[index_position][1]
        except IndexError:
            print("Key not found")
            raise KeyError
        else:
            # Update indirection column and scheme data
            byte_pos = (baseID-1) % PAGE_CAPACITY * DATA_SIZE
            base_pages = self.page_directory[baseID]
            prev_tid = int.from_bytes(base_pages[INDIRECTION_COLUMN].data[byte_pos:byte_pos + DATA_SIZE],'little')
            schema_data = base_pages[SCHEMA_ENCODING_COLUMN].data
            base_entry = int.from_bytes(schema_data[byte_pos:byte_pos + DATA_SIZE], byteorder="little")
            cur_record_pages[INDIRECTION_COLUMN].write(prev_tid if base_entry else baseID)
            base_pages[INDIRECTION_COLUMN].write(record.rid, byte_pos)
            base_schema_string = str(base_entry)
            if len(base_schema_string) < self.num_columns:
                base_schema_string = '0' * (self.num_columns - len(base_schema_string)) + base_schema_string
            updated_base_schema = ''
            for i in range(len(base_schema_string)):
                updated_base_schema += '1' if schema_encoding[i] == 1 else base_schema_string[i]
            base_pages[SCHEMA_ENCODING_COLUMN].write(int(updated_base_schema), byte_pos)

		# Write to rest of metadata columns
        cur_record_pages[RID_COLUMN].write(record.rid)
        cur_record_pages[TIMESTAMP_COLUMN].write(int(time()))
        # Wrote schema_encoding incorrectly
        schema_encoding_string = ''
        for bit in schema_encoding:
            schema_encoding_string += str(bit)
        schema_encoding_int = int(schema_encoding_string)
        cur_record_pages[SCHEMA_ENCODING_COLUMN].write(schema_encoding_int)
        
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
            byte_pos = (baseID - 1) % PAGE_CAPACITY * DATA_SIZE
            TID = int.from_bytes(self.page_directory[baseID][INDIRECTION_COLUMN].data[byte_pos:byte_pos+DATA_SIZE], 'little')
            if TID == 0:
                rid_output.append(baseID)
            else:
                rid_output.append(TID)
        
        #print("baseIDs in get latest = ", baseIDs)
        return rid_output

    def get_previous(self, tailID):
        rid_output = []
        byte_pos = abs(tailID - (2 ** 64 - 1)) % PAGE_CAPACITY * DATA_SIZE
        previous_RID = int.from_bytes(self.page_directory[tailID][INDIRECTION_COLUMN].data[byte_pos:byte_pos+DATA_SIZE], 'little')
        return previous_RID


    def read_records(self, key, query_columns):
        records = [self.indexer.index[index_position][1] for index_position in self.indexer.get_positions(key)]
        latest_records = self.get_latest(records)
        output = []
        '''
        return [Record(rid, key, [int.from_bytes(enumerated_page[1].data
                                                 [(rid- 1) % PAGE_CAPACITY * DATA_SIZE:(rid - 1) % PAGE_CAPACITY * DATA_SIZE + 8],
                                                 'little'
                                                 )
                                  if bool(query_columns[enumerated_page[0]]) else None
                                  for enumerated_page in enumerate(self.page_directory[rid][INIT_COLS:])])
                for rid in latest_records]
        '''
        for rid in latest_records:
            data = [None] * self.num_columns
            columns_not_retrieved = set()

            for i in range(len(query_columns)):
                if query_columns[i] == 1:
                    columns_not_retrieved.add(i)
            while len(columns_not_retrieved) > 0:
                # retrieve whatever data you can from latest record
                assert rid != 0
                # rid may be a base or a tail id
                # Tail id counts backwards so a single byte_pos formula won't work
                if rid >= self.TID_counter:
                    byte_pos = abs(rid - (2 ** 64 - 1)) % PAGE_CAPACITY * DATA_SIZE
                else:
                    byte_pos = (rid - 1) % PAGE_CAPACITY * DATA_SIZE
                schema = self.page_directory[rid][SCHEMA_ENCODING_COLUMN].data[byte_pos:byte_pos + DATA_SIZE]
                schema = str(int.from_bytes(schema, 'little'))
                if len(schema) < self.num_columns:
                    schema = '0' * (self.num_columns - len(schema)) + schema
                # leading zeros are lost in integer conversion
                # pad with zeros
                for i, page in enumerate(self.page_directory[rid][INIT_COLS:]):
                    if i not in columns_not_retrieved:
                        continue
                    # NOTE: retrieve values from older records if they aren't in the newest
                    if rid < self.TID_counter or bool(int(schema[i])):
                        data[i] = int.from_bytes(page.data[byte_pos:byte_pos + 8], 'little')
                        columns_not_retrieved.discard(i)
                # get rid from indirection column (if rid is a tail?)
                rid = self.get_previous(rid)
                if rid == 0:
                    break
            '''for i, page in enumerate(self.page_directory[rid][INIT_COLS:]):
                # rid may be a base or a tail id
                # Tail id counts backwards so a single byte_pos formula won't work
                if rid > self.TID_counter:
                    byte_pos = abs(rid-(2**64 - 1)) % PAGE_CAPACITY * DATA_SIZE
                else:
                    byte_pos = (rid-1) % PAGE_CAPACITY * DATA_SIZE
                schema = self.page_directory[rid][SCHEMA_ENCODING_COLUMN].data[byte_pos:byte_pos + DATA_SIZE]
                schema = str(int.from_bytes(schema,'little'))
                # leading zeros are lost in integer conversion
                # pad with zeros
                if len(schema) < self.num_columns:
                    schema = '0'*(self.num_columns - len(schema)) + schema
                # TODO: retrieve values from older records if they aren't in the newest
                if bool(query_columns[i]) and (rid < self.TID_counter or bool(int(schema[i]))):
                    data[i] = int.from_bytes(page.data[byte_pos:byte_pos + 8], 'little')
                    '''
            output.append(Record(rid, key, data))
        return output

    def collect_values(self, start_range, end_range, col_index):
        total = 0 # Accumulate column values
        byte_pos = 0
        page_data = None
        prev_rids = []
        latest_rids = []
       
        for key_val in range(start_range, end_range + 1):
            rid_batch = self.indexer.locate(key_val)
            if len(rid_batch): # At least one matching RID found (account for non-unique key values)
                prev_rids += rid_batch # Combine into one list
      
        latest_rids = self.get_latest(prev_rids)
       
        # Determine if original RIDs were the lastest ones
        check_equality = lambda prev, new: prev == new
        # Let 'bools' be a list of booleans (True: Identical & False: Updated)
        bools = list(map(check_equality, prev_rids, latest_rids))

        for i in range(len(latest_rids)):
            rid = latest_rids[i]



            while True:
                if rid < self.TID_counter:  # No updates made to record
                    byte_pos = (rid - 1) % PAGE_CAPACITY * DATA_SIZE
                else:  # Current RID is a TID
                    byte_pos = (rid - (2 ** 64 - 1)) % PAGE_CAPACITY * DATA_SIZE
                schema = self.page_directory[rid][SCHEMA_ENCODING_COLUMN].data[byte_pos:byte_pos + DATA_SIZE]
                schema = str(int.from_bytes(schema, 'little'))
                if len(schema) < self.num_columns:
                    schema = '0' * (self.num_columns - len(schema)) + schema
                # test
                page_data = self.page_directory[rid][INIT_COLS + col_index].data
                col_val = int.from_bytes(page_data[byte_pos:byte_pos + DATA_SIZE], 'little')
                if rid < self.TID_counter or int(schema[col_index]) == 1:
                    break
                rid = self.get_previous(rid)
            page_data = self.page_directory[rid][INIT_COLS + col_index].data
            col_val = int.from_bytes(page_data[byte_pos:byte_pos + DATA_SIZE], 'little')
            total += col_val

        return total

'''
	# Previous code: (incorrectly) assumed that start/end_ranges were indices of records
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
            byte_pos = (rid-1) % PAGE_CAPACITY * DATA_SIZE
            page_data = self.page_directory[rid][INIT_COLS + col_index].data
            col_val = int.from_bytes(page_data[byte_pos:byte_pos + DATA_SIZE], 'little')
            total += col_val

        return total
'''
