from template.page import *
from time import time
from template.config import *
from template.index import *

import math

class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Page_Range:

    def __init__(self, total_pages):
        # Track Page space
        self.last_base_row = 0
        self.last_tail_row = 0

        self.base_set = [] # List of Base Pages
        
        for _ in range(PAGE_RANGE_FACTOR):
            self.base_set.append([]) # List of empty lists
        
        self.tail_set = [] # List of Tail Pages
        self.num_updates = 0 # Number of Tail Records within Page Range

class Table:

    total_num_pages = 0

    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns

        self.page_directory = dict() # Maps RIDs : [page_range_index, page_row, byte_pos]
        self.page_range_collection = []
        self.indexer = Index(self)
        
        self.LID_counter = 0 # Used to increment LIDs
        self.TID_counter = (2 ** 64) - 1 # Used to decrement TIDs 


    """
    write_to_pages(): Conditionally writes to meta and user data columns
    """
    def write_to_pages(self, cur_pages, record, schema_encoding, isUpdate=None):
        # Write to metadata columns, if inserting base record
        if isUpdate is None:
            cur_pages[RID_COLUMN].write(record.rid)
            cur_pages[TIMESTAMP_COLUMN].write(int(time()))
            cur_pages[SCHEMA_ENCODING_COLUMN].write(int(schema_encoding))
        # Write to userdata columns
        for col in range(self.num_columns):
            entry = record.columns[col]
            if entry is not None:
                cur_pages[INIT_COLS + col].write(entry)
            else: # Write dummy value
                cur_pages[INIT_COLS + col].write(0)


    """
    insert_baseRecord(): Creates & inserts new base record into Table
    """
    def insert_baseRecord(self, record, schema_encoding):
        # Base case: Check if record's RID is unique
        if record.rid in self.page_directory.keys():
            print("Error: Record RID is not unique.\n")
            return

        # Init Values
        page_range = None
        total_pages = INIT_COLS + self.num_columns

        # Determine if corresponding Page Range exists
        page_range_index = (record.rid - 1)/(PAGE_RANGE_FACTOR * PAGE_CAPACITY)
        page_range_index = math.floor(page_range_index)
        
        try:
            page_range = self.page_range_collection[page_range_index]
        except:
            page_range = Page_Range(total_pages)
            self.page_range_collection.append(page_range)
        
        # Make alias
        page_range = self.page_range_collection[page_range_index]
        base_set = page_range.base_set # [[], [], ..., []]
        cur_base_pages = base_set[page_range.last_base_row] # []

        if len(cur_base_pages) == 0:
            # Init State, create set of Base Pages
            for base_row in range(PAGE_RANGE_FACTOR):
                for page in range(total_pages):
                    base_set[base_row].append(Page())
        elif not cur_base_pages[INIT_COLS].has_space():
            page_range.last_base_row += 1
        
        # Write to Base Pages within matching Range
        cur_base_pages = base_set[page_range.last_base_row]
        self.write_to_pages(cur_base_pages, record, schema_encoding)
         
        # Update Page Directory & Indexer
        byte_pos = cur_base_pages[INIT_COLS].first_unused_byte - DATA_SIZE
        self.page_directory[record.rid] = [page_range_index, page_range.last_base_row, byte_pos]
        # print("UPDATING PAGE DIRECTORY: { RID=", record.rid, " : index=", page_range_index, " & row=", page_range.last_base_row, "}\n")
        
        # TODO: Account for THIRD param in query Select -> may need to change Index() class
        self.indexer.insert(record.key, record.rid)


    """
    extend_tailSet(): Creates & appends a tailRow to current Page Range
    """
    def extend_tailSet(self, tail_set, total_pages):
        sublist = []
        for _ in range(total_pages):
            # Create a single Tail Row
            sublist.append(Page())
        tail_set.append(sublist)

    
    """
    insert_tailRecord(): Creates & inserts new tail record into Table
    """
    def insert_tailRecord(self, record, schema_encoding):
        # Retrieve base record with matching key
        baseID = self.indexer.locate(record.key)

        cur_keys = self.page_directory.keys()
        # Base case: Check if record's RID is unique & Page Range exists
        if record.rid in cur_keys or not baseID in cur_keys:
            print("Error: Tail Record RID is not unique or Page Range DNE.\n")
            return

        # Init Values
        page_range = None
        total_pages = INIT_COLS + self.num_columns

        # Locate Page Range of base/tail record
        [page_range_index, base_page_row, base_byte_pos] = self.page_directory[baseID]
        page_range = self.page_range_collection[page_range_index]

        tail_set = page_range.tail_set
        total_pages = INIT_COLS + self.num_columns
        if len(tail_set) == 0: # Init State
            self.extend_tailSet(tail_set, total_pages)
        elif not tail_set[page_range.last_tail_row][INIT_COLS].has_space():
            # Check if current Tail Page has space
            # (Can't combine conditions into one, otherwise indexing error)
            self.extend_tailSet(tail_set, total_pages)
            page_range.last_tail_row += 1

        cur_tail_pages = tail_set[page_range.last_tail_row] 

        # Write to userdata columns
        isUpdate = True
        self.write_to_pages(cur_tail_pages, record, schema_encoding, isUpdate)

        # Read from base_set's indirection column
        base_indir_page = page_range.base_set[base_page_row][INDIRECTION_COLUMN]
        base_indir_data = int.from_bytes(base_indir_page.data[base_byte_pos:base_byte_pos + DATA_SIZE], 'little')

        if base_indir_data: # Point to previous TID
            cur_tail_pages[INDIRECTION_COLUMN].write(base_indir_data)
        else: # Point to baseID
            cur_tail_pages[INDIRECTION_COLUMN].write(baseID)
        # Base Indirection now points to current TID (replacement)
        base_indir_page.write(record.rid, base_byte_pos)
        
        # Make alias
        base_schema_page = page_range.base_set[base_page_row][SCHEMA_ENCODING_COLUMN]
        tail_schema_page = cur_tail_pages[SCHEMA_ENCODING_COLUMN]
        
        # Write to tail schema column (non-cumulative)
        tail_schema = ''
        for bit in schema_encoding:
            tail_schema += str(bit)
        schema_int = int(tail_schema)
        tail_schema_page.write(schema_int)

        # Read from base schema
        schema_int = int.from_bytes(base_schema_page.data[base_byte_pos:base_byte_pos + DATA_SIZE], 'little')
        
        # Leading zeros lost after integer conversion, so padding needed
        init_base_schema = str(schema_int)
        final_base_schema = ''
        diff = self.num_columns - len(init_base_schema) 
        if diff:
            final_base_schema = ('0' * diff) + init_base_schema
        else:
            final_base_schema = init_base_schema
 
        # Merge tail & base schema
        latest_schema = ''
        for itr in range(len(tail_schema)):
            if int(tail_schema[itr]):
                latest_schema += '1'
            else:
                latest_schema += final_base_schema[itr]

        # Write to base schema column (cumulative)
        base_schema_page.write(int(latest_schema), base_byte_pos)

        # Write to RID & BaseRID, and timeStamp columns
        cur_tail_pages[RID_COLUMN].write(record.rid)
        cur_tail_pages[BASE_RID_COLUMN].write(baseID)
        cur_tail_pages[TIMESTAMP_COLUMN].write(int(time()))
   
        page_range.num_updates += 1
        byte_pos = cur_tail_pages[INIT_COLS].first_unused_byte - DATA_SIZE
        self.page_directory[record.rid] = [page_range_index, page_range.last_tail_row, byte_pos]
 
        # Update Indexer iff key value was changed
        new_key = record.columns[self.key_index]
        if new_key is not None:
            self.indexer.unique_update(record.key, new_key)
        # TODO: Account for THIRD param in query Select -> may need to change Index() class


    """
    read_record(): Reads record(s) with matching key value and indexing column
    """
    def read_records(self, key, query_columns, max_key=None):
        # NOTE: Assumes that primary key is being indexed
        # NOTE: Still need to update querySelect() parameters
        if max_key == None:
            try:
                records = [self.indexer.locate(key)]
            except KeyError:
                print("KeyError!\n")
                return
        else:
            #records = [self.indexer.index[index_position][1] for index_position in self.indexer.get_positions(key, max_key)]
            print("shouldn't print. Not working with querySum() yet, so skip this step for now..\n")
         #latest_records = self.get_latest(records) FOR NOW JUST TESTING BASE RECORDS INSERTIONS SO NO TAIL RECORDS YET

        output = []
         
        for rid in records:
            data = [None] * self.num_columns
            columns_not_retrieved = set()
             
            for i in range(len(query_columns)):
                if query_columns[i] == 1:
                    columns_not_retrieved.add(i)

            while len(columns_not_retrieved) > 0:
                # Retrieve whatever data you can from latest record
                assert rid != 0
                # RID may be a base or a tail ID
                # Tail ID counts backwards so a single byte_pos formula won't work
                
                """
                if rid >= self.TID_counter:
                    byte_pos = abs(rid - (2 ** 64 - 1)) % PAGE_CAPACITY * DATA_SIZE
                else:
                    byte_pos = (rid - 1) % PAGE_CAPACITY * DATA_SIZE
                """

                # assuming that we're just working with rid = base IDs ONLY
                [page_range_index, page_row, byte_pos] = self.page_directory[rid] # RIDs -> [page_range_index, page_row, byte_pos]
                page_range = self.page_range_collection[page_range_index]
                page_set = page_range.base_set[page_row] # NOTE: SHOULD BE IN IF ELSE STMT (determine if use base/tail_set)
                schema = page_set[SCHEMA_ENCODING_COLUMN].data[byte_pos:byte_pos + DATA_SIZE]
                # Leading zeros are lost in integer conversion, so padding needed
                schema = str(int.from_bytes(schema, 'little'))
                if len(schema) < self.num_columns:
                    schema = '0' * (self.num_columns - len(schema)) + schema
                for i, page in enumerate(page_set[INIT_COLS:]):
                    if i not in columns_not_retrieved:
                        continue
                    # Retrieve values from older records, if they are not in the newest
                    if rid < self.TID_counter or bool(int(schema[i])):
                        data[i] = int.from_bytes(page.data[byte_pos:byte_pos + DATA_SIZE], 'little')
                        columns_not_retrieved.discard(i)
                # Get RID from indirection column (if RID is tail?)
                # TODO: for now just working with base records
                """
                rid = self.get_previous(rid)
                if rid == 0: # Base Record reached
                    break
                """
            # End of while loop
            output.append(Record(rid, key, data))

        # End of outer for loop
        return output


    """
    merge(): Merges base & tail records within a Page Range
    """
    def __merge(self):
        pass

