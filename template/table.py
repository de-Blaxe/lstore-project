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
        # Track a Page's capacity
        self.last_base_row = 0
        self.last_tail_row = 0

        self.base_set = [] # List of Base Pages
        
        for _ in range(PAGE_RANGE_FACTOR):
            self.base_set.append([]) # List of empty lists
        
        self.tail_set = [] # List of Tail Pages
        self.num_updates = 0 # Count number of Tail Records within Page Range

class Table:

    total_num_pages = 0

    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns

        self.page_directory = dict() # Maps RIDS : [page_range_index, page_row]
        self.page_range_collection = []
        self.indexer = Index(self) # Passed self (Table instance) to Index constructor
        
        self.LID_counter = 0 # Used to increment LIDs
        self.TID_counter = (2 ** 64) - 1 # Used to decrement TIDs 

    def __merge(self):
        pass

    # Writes metadata and user input to set of Base Pages 
    def write_to_pages(self, cur_pages, record, schema_encoding, isUpdate=None):
        # Write to metadata columns, if inserting base record
        if isUpdate is None:
            cur_pages[RID_COLUMN].write(record.rid)
            cur_pages[TIMESTAMP_COLUMN].write(int(time()))
            cur_pages[SCHEMA_ENCODING_COLUMN].write(int(schema_encoding))

        # Write to userdata columns
        for col in range(self.num_columns):
            cur_pages[INIT_COLS + col].write(record.columns[col])

        """
        # DEBUGGING
        for page in range(self.num_columns + INIT_COLS):
            print("Page #", page, " has Num Records: ", cur_base_pages[page].num_records)
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

        # print("RID: ", record.rid, " BaseRow has Num BasePages: ", len(cur_base_pages), "\n")
        
        # Currently: base_set [[], [], [], ..., []] -> cur_base_pages []
        if len(cur_base_pages) == 0:
            # Init State, create set of Base Pages
            for base_row in range(PAGE_RANGE_FACTOR):
                for page in range(total_pages):
                    base_set[base_row].append(Page())
        elif not cur_base_pages[INIT_COLS].has_space():
            # print("RID=", record.rid, "Create & move on to next base page row ")
            page_range.last_base_row += 1
        
        cur_base_pages = base_set[page_range.last_base_row]

        # Write to Base Pages within matching Range
        self.write_to_pages(cur_base_pages, record, schema_encoding)
         
        # Update indexing for Page Directory & Indexer
        # print("UPDATING PAGE DIRECTORY: { RID=", record.rid, " : index=", page_range_index, " & row=", page_range.last_base_row, "}\n")
        self.page_directory[record.rid] = [page_range_index, page_range.last_base_row]
        self.indexer.insert(record.key, record.rid)


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
                
                if rid >= self.TID_counter:
                    byte_pos = abs(rid - (2 ** 64 - 1)) % PAGE_CAPACITY * DATA_SIZE
                else:
                    byte_pos = (rid - 1) % PAGE_CAPACITY * DATA_SIZE
               
                # assuming that we're just working with rid = base IDs ONLY
                [page_range_index, page_row] = self.page_directory[rid] # RIDs -> [page_range_index, page_row]
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

    def extend_tailSet(self, tail_set):
        sublist = []
        for _ in range(total_pages):
            # Create a single Tail Row
            sublist.append(Page())
        tail_set.append(sublist)


    def insert_tailRecord(self, record, schema_encoding):
        # Base case: Check if record's RID is unique & Page Range exists
        if record.rid in self.page_directory.keys():
            print("Error: Record RID is not unique.\n")
            return

        # Init Values
        page_range = None
        total_pages = INIT_COLS + self.num_columns

        # Retrieve base record with matching key
        baseID = self.indexer.locate(key)

        # Locate position of base/tail record
        [page_range_index, base_page_row] = self.page_directory[baseID]
 
        # Page Range must exist prior to an update
        page_range = self.page_range_collection[page_range_index]

        tail_set = page_range.tail_set # Could be empty if Init State
        total_pages = INIT_COLS + self.num_columns
        if len(tail_set) == 0: # is empty, []
            extend_tailSet(tail_set)
        elif not tail_set[INIT_COLS].has_space(): 
            # Check if current Tail Page has space
            # Can't combine conditions into one, otherwise indexing error
            extend_tailSet(tail_set)
            page_range.last_tail_row += 1

        # Make alias
        cur_tail_pages = tail_set[page_range.last_tail_row] 

        # print("RID: ", record.rid, " Row has Num TailPages: ", len(cur_tail_pages), "\n")

        # Write to userdata columns
        isUpdate = True
        self.write_to_pages(cur_tail_pages, record, schema_encoding, isUpdate)

        # Write to metadata columns:
        # Read from base_set's indirection column
        base_indir_page = base_set[base_page_row][INDIRECTION_COLUMN]
        base_byte_pos = (baseID - 1) % (PAGE_CAPACITY * DATA_SIZE)
        base_indir_data = int.from_bytes(base_indr_page.data[base_byte_pos:base_byte_pos + DATA_SIZE], 'little')
        if base_indir_data: # Point to previous TID
            cur_tail_pages[INDIRECTION_COLUMN].write(base_indir_data)
        else: # Point to baseID
            cur_tail_pages[INDIRECTION_COLUMN].write(baseID)
        # Base Indirection now points to current TID (replacement)
        base_indir_page.write(record.rid, base_byte_pos)
        
        # Update schema for both base and tail records
        base_schema_page = base_set[base_page_row][SCHEMA_ENCODING_COLUMN]
        tail_schema_page = cur_tail_pages[SCHEMA_ENCODING_COLUMN]
        
        #NOTES: 
        # write to baseRID column, timeStamp, RID, schema (convert)
        # update page_range.num_updates += 1 # help with merge selection
        # update page_directory for tail record ID
        # update indexer iff key value was changed
        
        # Account for THIRD param in query Select -> may need to change Index() class

