# NOTE: DEBUGGING VERSION 
from template.page import *
from time import time
from template.config import *
from template.index import *

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
        
        #init_set = [Page() for _ in range(total_pages)]
        init_set = []
        for _ in range(total_pages):
            init_set.append(Page())

        for _ in range(PAGE_RANGE_FACTOR):
            self.base_set.append(init_set)
        
        print("===length of page range===\n")
        print("num base rows: ", len(self.base_set), " & num base pages per row: ", len(self.base_set[0]), "\n")

        self.tail_set = [] # List of Tail Pages
        self.num_updates = 0

class Table:

    total_num_pages = 0

    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns

        self.page_directory = dict() # NOTE: Idea: RIDS -> [page_range_index, page_row]
        self.page_range_collection = []
        self.indexer = Index(self) # Passed self (Table instance) to Index constructor
        
        self.LID_counter = 0 # Used to increment LIDs
        self.TID_counter = (2 ** 64) - 1 # Used to decrement TIDs 

        # Keep track of available base and tail pages
        #NOTE: REPLACED THESE WITH PAGE_RANGE.LAST_BASE/TAIL_ROW
        #self.last_LID_used = -1
        #self.last_TID_used = -1

    def __merge(self):
        pass

    # Writes metadata and user input to set of Base Pages 
    def write_to_basePages(self, cur_base_pages, record, schema_encoding):
        # Write to metadata columns
        cur_base_pages[RID_COLUMN].write(record.rid)
        cur_base_pages[TIMESTAMP_COLUMN].write(int(time()))
        cur_base_pages[SCHEMA_ENCODING_COLUMN].write(int(schema_encoding))

        # Write to userdata columns
        for col in range(self.num_columns):
            cur_base_pages[INIT_COLS + col].write(record.columns[col])

    def insert_baseRecord(self, record, schema_encoding):
        # Base case: Check if record's RID is unique
        if record.rid in self.page_directory.keys():
            print("Error: Record RID is not unique.\n")
            return

        # Init Values
        page_range = None
        total_pages = INIT_COLS + self.num_columns

        # Determine if corresponding Page Range exists
        #page_range_index = (PAGE_RANGE_FACTOR * PAGE_CAPACITY) % record.rid
        page_range_index = 0
        breakpoint = PAGE_RANGE_FACTOR * PAGE_CAPACITY
        while(1): 
        # NOTE: THIS MAY HURT PERFORMANCE BC INSERTIONS ARE SEQUENTIAL IN MAIN() TESTER
            if record.rid <= breakpoint:
               break
            #print("while loop RID: ", record.rid, "HELLO ")
            page_range_index += 1
            breakpoint = breakpoint * (page_range_index + 1)
        
        try:
            page_range = self.page_range_collection[page_range_index]
        except:
            page_range = Page_Range(total_pages)
            self.page_range_collection.append(page_range)
        
        # Make alias
        page_range = self.page_range_collection[page_range_index]
        base_set = page_range.base_set # [[BasePages], [BasePages], ..., [BasePages]]
        cur_base_pages = base_set[page_range.last_base_row] # [BasePages]

        # Determine capacity of a Base Page within Range
        for row in range(PAGE_RANGE_FACTOR):
            for page in range(total_pages):
                print("Num Records in Page=", page, ": ", base_set[row][page].num_records)
            print("Starting next Base Row")
        
        if not cur_base_pages[INIT_COLS].has_capacity():
            print("---Base Page has ", cur_base_pages[INIT_COLS].num_records, "\n")
            #print("RID= ", record.rid, " NEED TO ALLOCATE NEW SET OF BASE PAGES\n")
            print("RID=", record.rid, "move on to next base page row (already allocated by Page_Range())--\n")
            # Append new list of Base Pages
            #base_set.append([Page() for _ in range(total_pages)])
            page_range.last_base_row += 1

        cur_base_pages = base_set[page_range.last_base_row]

        # Write to Base Pages within matching Range
        self.write_to_basePages(cur_base_pages, record, schema_encoding)
         
        # Update indexing for Page Directory & Indexer
        print("UPDATING PAGE DIRECTORY: { RID=", record.rid, " : index=", page_range_index, " & row=", page_range.last_base_row, "}\n")
        self.page_directory[record.rid] = [page_range_index, page_range.last_base_row] # NOTE: JC IDEA
        self.indexer.insert(record.key, record.rid)

        # TESTING
        print("Page Range collection[0] length: ", len(self.page_range_collection[0].base_set), "\n") 
        # Gives 4, then 8 (1000 records-> 1.9 base_sets needed)

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
