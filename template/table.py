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
            self.base_set.append([])
        
        self.tail_set = []   # List of Tail Pages
        self.num_updates = 0 # Number of Tail Records within Page Range

class Table:

    total_num_pages = 0

    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns

        # Page Directory        - Maps RIDs to [page_range_index, page_row, byte_pos]
        # Page Range Collection - Stores all Page Ranges for Table
        # Indexer               - Maps key values to baseIDs
        self.page_directory = dict()
        self.page_range_collection = []
        self.indexer = Index(self)
        
        self.LID_counter = 0             # Used to increment LIDs
        self.TID_counter = (2 ** 64) - 1 # Used to decrement TIDs 


    """
    # Conditionally writes to meta and user data columns
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
                # Create index on the column
            else: # Write dummy value
                cur_pages[INIT_COLS + col].write(0)


    """
    # Creates & inserts new base record into Table
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
        
        # Insert key, vals for each column
        for col_num, val in enumerate(record.columns):
            self.indexer.create_index(val, record.rid, col_num)

    """
    # Creates & appends a tailRow to current Page Range
    """
    def extend_tailSet(self, tail_set, total_pages):
        sublist = []
        for _ in range(total_pages):
            # Create a single Tail Row
            sublist.append(Page())
        tail_set.append(sublist)

    
    """
    # Creates & inserts new tail record into Table
    """
    def insert_tailRecord(self, record, schema_encoding):
        # Retrieve base record with matching key
        baseID = self.indexer.locate(record.key, self.key_index)

        cur_keys = self.page_directory.keys()
        # Base case: Check if record's RID is unique & Page Range exists
        if record.rid in cur_keys or not baseID in cur_keys:
            return

        # Init Values
        page_range = None
        total_pages = INIT_COLS + self.num_columns

        # Locate Page Range of base/tail record
        [page_range_index, base_page_row, base_byte_pos] = self.page_directory[baseID]
        page_range = self.page_range_collection[page_range_index]

        tail_set = page_range.tail_set
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

        # Store prev data --> Prev RID -> read the data for record = RID
        prev_rid = baseID
        if base_indir_data: # Point to previous TID
            cur_tail_pages[INDIRECTION_COLUMN].write(base_indir_data)
            prev_rid = base_indir_data

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
        
        
        # locate previous record 
        [_, prev_row, prev_byte_pos] = self.page_directory[prev_rid]

        #print("Prev RID = ", prev_rid, "Prev row = ", prev_row, "Prev byte pos = ", prev_byte_pos)

        # Check if RID is base or tail
        prev_set = page_range.base_set if prev_rid < self.TID_counter else page_range.tail_set
        
        # TODO: Account for THIRD param in query Select -> may need to change Index() class
        # Update indexer entries if needed
        for col_num, value in enumerate(record.columns):
            if value is not None:
                # Find old value
                #print("Column number = ", col_num)
                prev_data = prev_set[prev_row][col_num].data
                #print("Prev byte array = ", prev_data)
                prev_key = int.from_bytes(prev_data[prev_byte_pos: prev_byte_pos+DATA_SIZE], 'little')
                #print("Prev key = ", prev_key)

                # New key value = record.columns[bit]
                new_key = record.columns[bit]
                # Call the update index function
                self.indexer.update_index(prev_key, new_key, col_num)

    """
    # Given list of baseIDs, retrieves corresponding latest RIDs
    """
    def get_latest(self, baseIDs):
        rid_output = [] # List of RIDs
        if isinstance(baseIDs, int):
            baseIDs = list(baseIDs)
        
        for baseID in baseIDs:
            # Retrieve value in base record's indirection column
            [page_range_index, base_page_row, base_byte_pos] = self.page_directory[baseID]
            base_set = self.page_range_collection[page_range_index].base_set
            base_indir_page = base_set[base_page_row][INDIRECTION_COLUMN]
            latest_RID = int.from_bytes(base_indir_page.data[base_byte_pos:base_byte_pos + DATA_SIZE], 'little') 
            if latest_RID == 0: # No updates made
                rid_output.append(baseID)
            else: # Base record has been updated
                rid_output.append(latest_RID)

        return rid_output

    
    """
    # Given a RID, retrieves corresponding previous RID, if any
    """
    def get_previous(self, rid):
        # Retrieve value in tail record's indirection column
        [page_range_index, page_row, byte_pos] = self.page_directory[rid]    
        page_range = self.page_range_collection[page_range_index]

        tail_set = page_range.tail_set      
        if len(tail_set) == 0 or rid < self.TID_counter: 
            # No updates made to page_range, or rid=baseID
            return 0 # Indicate that current rid is latest (base)RID
        else:
            tail_indir_page = tail_set[page_row][INDIRECTION_COLUMN]
            prev_RID = int.from_bytes(tail_indir_page.data[byte_pos:byte_pos + DATA_SIZE], 'little')
            return prev_RID # Single RID


    """
    # Reads record(s) with matching key value and indexing column
    """
    def read_records(self, key, column, query_columns, max_key=None): 
         if max_key == None:
             try:
                 baseIDs = [self.indexer.locate(key, column)]
             except KeyError:
                 print("KeyError!\n")
                 return
         # else: # Reading multiple records 
         # TODO Put code back in
         #    baseIDs = [self.indexer.index[index_pos][1] for index_pos in self.indexer.get_positions(key, max_key)]
   
   
         latest_records = self.get_latest(baseIDs)
         output = [] # A list of Record objects to return
         
         for rid in latest_records:
            data = [None] * self.num_columns 
            columns_not_retrieved = set()
 
            # Determine columns not retrieved yet
            for i in range(len(query_columns)):  
                if query_columns[i] == 1:
                    columns_not_retrieved.add(i)
 
            while len(columns_not_retrieved) > 0: 
                # Retrieve whatever data you can from latest record
                assert rid != 0
                # Locate record within Page Range
                [page_range_index, page_row, byte_pos] = self.page_directory[rid]
                page_range = self.page_range_collection[page_range_index]
 
                # RID may be a base or a tail ID
                if rid >= self.TID_counter:
                    page_set = page_range.tail_set
                else:
                    page_set = page_range.base_set # go here instead
 
                # Read schema data
                schema_page = page_set[page_row][SCHEMA_ENCODING_COLUMN]
                schema_data = schema_page.data[byte_pos:byte_pos + DATA_SIZE]
                schema_str = str(int.from_bytes(schema_data, 'little'))
 
                # Leading zeros are lost after integer conversion, so padding needed
                if len(schema_str) < self.num_columns:
                    diff = self.num_columns - len(schema_str)
                    schema_str = '0' * diff + schema_str
 
                for col, page in enumerate(page_set[page_row][INIT_COLS:]):
                    if col not in columns_not_retrieved:
                        continue # shouldnt happend bc all col in columns_not_retrieved
                    # Retrieve values from older records, if they are not in the newest ones
                    if rid < self.TID_counter or bool(int(schema_str[col])): # rid=2 ** 64 -1
                        data[col] = int.from_bytes(page.data[byte_pos:byte_pos + DATA_SIZE], 'little')
                        columns_not_retrieved.discard(col) # data = [91678, 100, 99] , set = [0]
 
                # Get RID from indirection column
                prev_rid = self.get_previous(rid)
                if prev_rid == 0:  
                    break # Base record encountered
                else:
                    rid = prev_rid # Follow lineage
 
            # End of while loop
            record = Record(rid, key, data)
            output.append(record)
 
        # End of outer for loop
         return output


    """
    # Merges base & tail records within a Page Range
    """
    def __merge(self):
        pass
