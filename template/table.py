from template.page import *
from time import time
from template.config import *
from template.index import *

import math
import operator

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

    def __init__(self, name="defaultName", num_columns=1, key_index=0):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns

        # Page Directory        - Maps RIDs to [page_range_index, page_row, byte_pos, pageSet_name]
        # Page Range Collection - Stores all Page Ranges for Table
        # Indexer               - Maps key values to baseIDs
        self.page_directory = dict()
        self.page_range_collection = []
        self.indexer = Index(self)
        
        self.LID_counter = 0             # Used to increment LIDs
        self.TID_counter = (2 ** 64) - 1 # Used to decrement TIDs 

        self.invalid_rids = []
        self.update_to_pg_range = dict()

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
            else: # Write dummy value
                cur_pages[INIT_COLS + col].write(0)


    """
    # Fetch & convert binary data into integer form
    """
    def convert_data(self, page, byte_pos):
        converted_data = int.from_bytes(page.data[byte_pos:byte_pos + DATA_SIZE], 'little')
        return converted_data # Single integer


    """
    # Returns finalized schema encoding, given Record's position in Page Range
    """
    def finalize_schema(self, schema_page, byte_pos):
        schema_data = self.convert_data(schema_page, byte_pos)
        init_schema = str(schema_data)
        # Determine amount of padding needed (diff), if any
        diff = self.num_columns - len(init_schema)
        final_schema = ('0' * diff) + init_schema if diff else init_schema
        return [final_schema, diff] # diff result used in merge()


    """
    # Creates & inserts new base record into Table
    """
    def insert_baseRecord(self, record, schema_encoding):
        # Base case: Check if record's RID is unique
        if record.rid in self.page_directory:
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
        self.page_directory[record.rid] = [page_range_index, page_range.last_base_row, byte_pos, "DEFAULT"]
        # print("UPDATING PAGE DIRECTORY: ")
        # print("{ RID=", record.rid, " : index=", page_range_index")
        # print(" & row=", page_range.last_base_row, "}\n")
        
        # Insert primary key: RID for key_index column
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
        baseID = self.indexer.locate(record.key, self.key_index)
        if baseID == INVALID_RECORD:
            return
        else:
            cur_keys = self.page_directory
            # Base case: Check if record's RID is unique & Page Range exists
            if record.rid in cur_keys or not baseID in cur_keys:
                print("Error: Record RID is not unique.\n")
                return
    
            # Init Values
            page_range = None
            total_pages = INIT_COLS + self.num_columns
    
            # Locate Page Range of base/tail record
            [page_range_index, base_page_row, base_byte_pos, _] = self.page_directory[baseID]
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
            base_indir_data = self.convert_data(base_indir_page, base_byte_pos)
            
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
    
            [final_base_schema, _] = self.finalize_schema(base_schema_page, base_byte_pos)
     
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
            self.update_to_pg_range[page_range_index] = page_range.num_updates

            byte_pos = cur_tail_pages[INIT_COLS].first_unused_byte - DATA_SIZE 
            self.page_directory[record.rid] = [page_range_index, page_range.last_tail_row, byte_pos, "DEFAULT"]
                   
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
            [page_range_index, base_page_row, base_byte_pos, _] = self.page_directory[baseID]
            base_set = self.page_range_collection[page_range_index].base_set
            base_indir_page = base_set[base_page_row][INDIRECTION_COLUMN]
            latest_RID = self.convert_data(base_indir_page, base_byte_pos) 
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
        [page_range_index, page_row, byte_pos, _] = self.page_directory[rid]    
        page_range = self.page_range_collection[page_range_index]

        tail_set = page_range.tail_set      
        if len(tail_set) == 0 or rid < self.TID_counter: 
            # No updates made to page_range, or rid=baseID
            return 0 # Indicate that current rid is latest (base)RID
        else:
            tail_indir_page = tail_set[page_row][INDIRECTION_COLUMN]
            prev_RID = self.convert_data(tail_indir_page, byte_pos)
            return prev_RID # Single RID


    """
    # Reads record(s) with matching key value and indexing column
    """
    def read_records(self, keys, column, query_columns, max_key=None):
         baseIDs = []
         if max_key == None:
            if column != self.key_index:
                for key in keys:
                    result = self.indexer.locate(key, column)
                    # indexer.locate() returns flag if no match found
                    if isinstance(result, int):
                        if result == INVALID_RECORD:
                            continue # Don't append invalid RID
                    baseIDs += result
                # Sort duplicate keys
                baseIDs = sorted(baseIDs)
            else:
                # If column is primary key, we get a single key with single base RID
                result = self.indexer.locate(keys, column)
                if result != INVALID_RECORD:
                    baseIDs.append(result)
         else: # Performing multi reads for summation 
            baseIDs = self.indexer.locate_range(keys, max_key, column)
         
         latest_records = self.get_latest(baseIDs)
         output = [] # A list of Record objects to return
         
         for rid in latest_records:
            # Validation Stage: Check if rid is invalid
            if rid in self.invalid_rids:
                continue # Go to next rid in latest_records

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
                [page_range_index, page_row, byte_pos, _] = self.page_directory[rid]
                page_range = self.page_range_collection[page_range_index]
 
                # RID may be a base or a tail ID
                if rid >= self.TID_counter:
                    page_set = page_range.tail_set
                else:
                    page_set = page_range.base_set
 
                # Read schema data
                schema_page = page_set[page_row][SCHEMA_ENCODING_COLUMN]
                [schema_str, _] = self.finalize_schema(schema_page, byte_pos)
 
                for col, page in enumerate(page_set[page_row][INIT_COLS:]):
                    if col not in columns_not_retrieved:
                        continue
                    # Retrieve values from older records, if they are not in the newest ones
                    if rid < self.TID_counter or bool(int(schema_str[col])):
                        data[col] = self.convert_data(page, byte_pos)
                        columns_not_retrieved.discard(col)
 
                # Get RID from indirection column
                prev_rid = self.get_previous(rid)
                if prev_rid == 0:  
                    break # Base record encountered
                else:
                    rid = prev_rid # Follow lineage
 
            ### End of while loop ###
            # Given key might not be a primary key
            primary_key = data[self.key_index]
            # Append each RID's record into a list
            record = Record(rid, primary_key, data)
            output.append(record)
 
         ### End of outer for loop ###
         return output


    """
    # Called only on primary key column
    """          
    def collect_values(self, start_range, end_range, col_index):
        # Base case: Check if col_index in range
        if col_index < 0 or col_index >= self.num_columns:
            print("Error: Specified column index out of range.\n")
            return
        # Init values
        total = 0      
        query_columns = [0] * self.num_columns
        query_columns[col_index] = 1

        records = self.read_records(start_range, col_index, query_columns, end_range)
        for record in records:
            total += record.columns[col_index]
        return total


    """
    # Delete Record with matching key. Assumes primary key is given.
    """
    def delete_record(self, key):
        # Retrieve matching baseID for given key
        baseID = self.indexer.locate(key, self.key_index)
        [page_range_index, page_row, byte_pos, _] = self.page_directory[baseID]        

        # Invalidate base record
        page_range = self.page_range_collection[page_range_index]
        cur_base_pages = page_range.base_set[page_row] # Single []
        base_rid_page = cur_base_pages[RID_COLUMN]
        base_rid_page.write(INVALID_RECORD, byte_pos)

        # Invalidate all tail records, if any
        next_rid = self.get_latest([baseID])[0] # get_latest() returns a list
        num_deleted = 0

        # Add either baseID or latest tailID
        representative = next_rid if next_rid != 0 else baseID
        self.invalid_rids.append(representative)

        # Start from most recent tail records to older ones      
        while (next_rid != 0): # At least one tail record exists
            """
            # I don't think it's beneficial to mark their RIDs as invalid
            # But still need to get num_deleted/invalidated
            [_, page_row, byte_pos, _] = self.page_directory[next_rid]
            cur_tail_pages = page_range.tail_set[page_row]
            tail_rid_page = cur_tail_pages[RID_COLUMN]
            tail_rid_page.write(INVALID_RECORD, byte_pos)
            """
            num_deleted += 1
            next_rid = self.get_previous(next_rid)

        # Update Indexer & Page Range Collection
        self.indexer.indices[self.key_index].remove(key)
        page_range.num_updates -= num_deleted


    """
    # Merges base & tail records within a Page Range
    """
    def __merge(self):

        merge_queue = []

        # Check if all Page Ranges have already been merged
        if sum(list(self.update_to_pg_range.values())) != 0:
            # Select Page Range with most number of updates
            page_range_index = max(self.update_to_pg_range.items(), key=operator.itemgetter(1))[0]

            # Collect Tail Pages within Page Range
            page_range = self.page_range_collection[page_range_index]
            tail_set = page_range.tail_set # [[Page, Page, Page],[Page, Page, Page], ...]
            base_set_copy = page_range.base_set.copy() # [[],[]]

            merge_queue = tail_set # List of Tail Rows
            last_TID_merged = 0 # Acts as TPS value

            # Remaining Work Dictionary - Maps baseIDs to [columns needed, TPS, visited flag]
            remaining_work = defaultdict(list)

            # Init remaining work dictionary
            # Valid baseIDs start at 1
            minRID = (page_range_index) * (PAGE_RANGE_FACTOR * PAGE_CAPACITY) + 1
            maxRID = minRID + (PAGE_RANGE_FACTOR * PAGE_CAPACITY) - 1

            # Only write once per column per base record
            init_tps = 0
            tps_index = self.num_columns
            wasVisited = False
            visited_index = tps_index + 1

            # Init Remaining Work Dictionary 
            for baseID in range(minRID, maxRID + 1):
                remaining_work[baseID] = [col for col in range(self.num_columns)]
                remaining_work[baseID] += [init_tps, wasVisited]

            # Create consolidated Base Pages
            for row_number, tail_row in enumerate(merge_queue):
                # Read Tail Page schema & TID
                tail_schema_page = tail_row[SCHEMA_ENCODING_COLUMN]
                # Byte positions are aligned across all Pages
                last_byte_pos = tail_schema_page.first_unused_byte - DATA_SIZE

                # Iterate Tail Records backwards
                while(last_byte_pos >= 0):
                    # Find mapped baseID for tail record
                    mapped_base_page = tail_row[BASE_RID_COLUMN]
                    mapped_baseID = self.convert_data(mapped_base_page, last_byte_pos)

                    # Locate Base Record within selected Page Range
                    [_, base_row, base_byte_pos, _] = self.page_directory[mapped_baseID]

                    # if time, make a string to padded schema converter/subfxn
                    if remaining_work[mapped_baseID][visited_index] == False:
                        base_schema_page = base_set_copy[base_row][SCHEMA_ENCODING_COLUMN]
                        [final_base_schema, diff] = self.finalize_schema(base_schema_data, base_byte_pos)
                        # Remove non-updated columns from remaining work dictionary
                        for column, char in enumerate(final_base_schema):
                            if char == '0':
                                remaining_work[mapped_baseID].remove(column)
                        # Base Record has now been visited
                        remaining_work[mapped_baseID][visited_index] = True

                    # Check remaining work and TPS for encountered baseID
                    base_tps = remaining_work[mapped_baseID][tps_index]
                    # Fetch current TID                    
                    tail_rid_page = tail_row[RID_COLUMN]
                    curr_TID = self.convert_data(tail_rid_page, last_byte_pos)
                    
                    non_user_cols = 2 # TPS + visited Flag
                    if len(remaining_work[mapped_baseID]) == non_user_cols or base_tps == curr_TID:
                        # Finished one consolidated base record OR current Tail Record already merged
                        last_byte_pos -= DATA_SIZE
                        continue
                    else:
                        # Keep overwriting TPS column for each Base Record
                        last_TID_merged = curr_TID
                        base_set_copy[base_row][TPS_COLUMN].write(last_TID_merged, base_byte_pos)
                
                        tail_schema = self.convert_data(tail_schema, last_byte_pos)
                        diff = self.num_columns - len(tail_schema)

                        # Because of padding, we know that columns < diff were not updated
                        for offset in range(diff, self.num_columns):
                            # A single Tail Record can update 1+ columns
                            if tail_schema[offset] == '1':
                                base_page = base_set_copy[base_row][INIT_COLS + offset]
                                tail_data = tail_pages[INIT_COLS + offset].data
                                # Overwrite base set copy data
                                base_page.write(tail_data, base_byte_pos)
                                remaining_work[mapped_baseID].remove(offset)

                        # Fetch earlier Tail Record
                        last_byte_pos -= DATA_SIZE

            ### After merge ###
            # Set selected Page Range's num_updates = 0
            page_range.num_updates = 0

            # Two copies coexisting: original -> bufferpool and merged -> page range
            for row_number, base_row in enumerate(base_set_copy): # [[],[]]
                page_range.base_set[row_number] = base_row

        # Else, busy wait until job is available
