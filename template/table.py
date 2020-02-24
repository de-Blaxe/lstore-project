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
        
        self.indexer.insert_primaryKey(record.key, record.rid)

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
        baseID = self.indexer.locate(record.key, self.key_index) # Modified to account for return value=-1 (no match)
        if baseID == INVALID_RECORD:
            return # Bypass logic error in main()
        else:
            cur_keys = self.page_directory.keys()
            # Base case: Check if record's RID is unique & Page Range exists
            if record.rid in cur_keys or not baseID in cur_keys:
                print("Error: Record RID is not unique.\n")
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
            diff = self.num_columns - len(init_base_schema)
            final_base_schema = ('0' * diff) + init_base_schema if diff else init_base_schema
     
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
       
            # Increment number of updates and update page directory
            page_range.num_updates += 1
            byte_pos = cur_tail_pages[INIT_COLS].first_unused_byte - DATA_SIZE 
            self.page_directory[record.rid] = [page_range_index, page_range.last_tail_row, byte_pos]
                   
            # Check if primary key is updated -- if it is then replace old key with new key 
            if record.columns[self.key_index] is not None:
                self.indexer.update_primaryKey(record.key, record.columns[self.key_index], self.key_index)
                

    """
    # Given list of baseIDs, retrieves corresponding latest RIDs
    """
    def get_latest(self, baseIDs):
        rid_output = [] # List of RIDs
        if isinstance(baseIDs, int):
            baseIDs = [baseIDs]
        
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
            baseIDs = self.indexer.locate(key, column)
            if isinstance(baseIDs, int):
                if baseIDs == INVALID_RECORD:
                    print("Read Error: Invalid Record\n")
                    return
        else: # Reading multiple records
            baseIDs = self.indexer.locate_range(key, max_key, column)

        # Init values
        latest_records = self.get_latest(baseIDs)
        output = [] # A list of Record objects to return
        mappings = dict() # Maps key to Record object
 
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
 
                # Determine Page Set: RID may be a base or a tail ID
                page_set = page_range.tail_set if rid >= self.TID_counter else page_range.base_set
 
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
                        continue
                    # Retrieve values from older records, if they are not in the newest ones
                    if rid < self.TID_counter or bool(int(schema_str[col])): 
                        data[col] = int.from_bytes(page.data[byte_pos:byte_pos + DATA_SIZE], 'little')
                        columns_not_retrieved.discard(col)
 
                # Get RID from indirection column
                prev_rid = self.get_previous(rid)
                if prev_rid == 0:
                    break # Base record encountered
                rid = prev_rid # Otherwise, follow lineage
 
            # End of while loop
            primary_key = data[self.key_index]
            record = Record(rid, primary_key, data) # NOTE: Now we can pass any 'key' (not just primary)
            mappings[primary_key] = record
 
        # Sort from smallest SID to largest SID -> consistent output (I guess it's not really needed tho)
        for sorted_index in sorted(mappings.keys()):
            output.append(mappings[sorted_index])
            
        # End of outer for loop
        # Debugging Print stmts
        if column != 0:
            print("Selected Col=", column, " and Key Value for that Col=", key)
            for i in range(len(mappings)):
                print("Matching Record: ", output[i].columns)
            print("===================")
        return output


    # Called only on primary key column            
    def collect_values(self, start_range, end_range, col_index):
        # Base case: Check if col_index in range
        if col_index < 0 or col_index >= self.num_columns:
            print("Error: Specified column index out of range.\n")
            return
        
        total = 0
        query_columns = [0] * self.num_columns
        query_columns[col_index] = 1

        records = self.read_records(start_range, col_index, query_columns, end_range)
        for record in records:
            total += record.columns[col_index]
        return total


    """
    # Merges base & tail records within a Page Range
    """
    def __merge(self):
        pass
