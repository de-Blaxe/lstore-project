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
        init_set = [Page() for _ in range(total_pages)]
        for _ in range(PAGE_RANGE_FACTOR):
            self.base_set.append(init_set)
        self.tail_set = [] # List of Tail Pages
        self.num_updates = 0

class Table:

    total_num_pages = 0

    def __init__(self, name, num_columns, key_index):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns
        self.page_directory = dict()
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
            print("while loop RID: ", record.rid, "HELLO ")
            page_range_index += 1
            breakpoint = breakpoint * (page_range_index + 1)
        
        print("RID: ", record.rid)
        print("PAGE RANGE INDEX : ", page_range_index)

        #####if not self.page_range_collection[page_range_index]:
        ###if len(self.page_range_collection) == 0 or not self.page_range_collection[page_range_index]:
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
        if not cur_base_pages[RID_COLUMN].has_capacity():
            # Append new list of Base Pages
            base_set.append([Page() for _ in range(total_pages)])
            page_range.last_base_row += 1
            cur_base_pages = base_set[page_range.last_base_row]

        # Write to Base Pages within matching Range
        self.write_to_basePages(cur_base_pages, record, schema_encoding)
         
        # Update indexing for Page Directory & Indexer
        self.page_directory[record.rid] = page_range.last_base_row
        self.indexer.insert(record.key, record.rid)
