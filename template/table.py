from template.page import *
from time import time
from template.config import *
from template.index import *
from copy import deepcopy

import threading
import math
import operator


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Page_Range:

    def __init__(self, num_page_range, table):
        # Track Page space through indices
        self.last_base_name = 0
        self.last_tail_name = 0

        self.base_set = [] # List of Base Page Set Names
        for i in range(PAGE_RANGE_FACTOR):
            # Page Set Names Format: 'firstBaseID_TableName'
            delimiter = '_'
            first_baseID = str(num_page_range*PAGE_CAPACITY*PAGE_RANGE_FACTOR + i*PAGE_CAPACITY)
            encoded_base_set = first_baseID + delimiter + table.name
            table.memory_manager.create_page_set(encoded_base_set, table=table)
            self.base_set.append(encoded_base_set)

        self.tail_set = []   # List of Tail Page Set Names
        self.num_updates = 0 # Number of Tail Records within Page Range

class Table:

    def __init__(self, name, num_columns, key_index, mem_manager, lock_manager):
        self.name = name
        self.key_index = key_index
        self.num_columns = num_columns

        self.page_directory = dict()        # Maps RIDs to [page_range_index, name_index, byte_pos]
        self.page_range_collection = []     # Stores all Page Ranges for Table
        self.index = Index(self)            # Index object that maps key values to baseIDs

        self.LID_counter = 0                # Used to increment LIDs
        self.TID_counter = (2 ** 64) - 1    # Used to decrement TIDs
        self.invalid_rids = []

        self.update_to_pg_range = dict()    # Entries: Number of Tail Records within Page Range
        self.memory_manager = mem_manager   # All Tables within Database share same Memory Manager
        self.lock_manager = lock_manager    # Manages concurrent Threads 

        # DEBUGGING PURPOSES: Assuming only 8 Transaction Worker Threads
        # https://www.geeksforgeeks.org/ways-increment-character-python/        
        self.all_threads = dict() # Map ThreadIDs to a nickname 
        self.thread_count = 0 # Counter
        self.thread_nickname = 'A' # Thread nicknames: 'A' thru 'H'

        self.merge_flag = False # TODO: Set flag to True in merge()
        self.num_merged = 0 
        self.merge_queue = []
        # Generate MergeThread in background
        thread = threading.Thread(target=self.__merge, args=[])
        # After some research, reason why we need daemon thread
        # https://www.bogotobogo.com/python/Multithread/python_multithreading_Daemon_join_method_threads.php
        thread.setDaemon(True)
        thread.start()


    """
    # Conditionally writes to meta and user data columns
    """
    def write_to_pages(self, cur_pages, record, schema_encoding, page_set_name, isUpdate=None):
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
        self.memory_manager.setDirty(page_set_name)


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
        return [final_schema, diff]


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
            page_range = Page_Range(len(self.page_range_collection), self)
            self.page_range_collection.append(page_range)

        # Make alias
        page_range = self.page_range_collection[page_range_index]
        base_set = page_range.base_set  # List of Base Set Names
        cur_base_pages = self.memory_manager.get_pages(base_set[page_range.last_base_name], table=self) 

        self.memory_manager.exclusiveLocks[base_set[page_range.last_base_name]].acquire()
        cur_base_pages_space = cur_base_pages[INIT_COLS].has_space()
        if not cur_base_pages_space:
            self.memory_manager.unpinPages(base_set[page_range.last_base_name])
            self.memory_manager.exclusiveLocks[base_set[page_range.last_base_name]].release()
            page_range.last_base_name += 1
            cur_base_pages = self.memory_manager.get_pages(base_set[page_range.last_base_name], 
                                                            table=self, read_only=False)
            self.memory_manager.exclusiveLocks[base_set[page_range.last_base_name]].acquire()
        # Write to Base Pages within matching Range
        self.write_to_pages(cur_base_pages, record, schema_encoding, page_set_name=base_set[page_range.last_base_name])
         
        # Update Page Directory & Indexer
        byte_pos = cur_base_pages[INIT_COLS].first_unused_byte - DATA_SIZE
        self.memory_manager.setDirty(base_set[page_range.last_base_name])
        self.memory_manager.unpinPages(base_set[page_range.last_base_name])
        self.memory_manager.exclusiveLocks[base_set[page_range.last_base_name]].release()
        self.page_directory[record.rid] = [page_range_index, page_range.last_base_name, byte_pos]
        
        # Insert primary key: RID for key_index column
        self.index.insert_primaryKey(record.key, record.rid)


    """
    # Creates & appends a List of Tail Page Set Names to current Page Range
    """
    def extend_tailSet(self, tail_set, first_rid):
        delimiter = '_'
        encoded_tail_set = str(first_rid) + delimiter + self.name
        self.memory_manager.create_page_set(encoded_tail_set, table=self)
        tail_set.append(encoded_tail_set)

    
    """
    # Creates & inserts new tail record into Table
    """
    def insert_tailRecord(self, record, schema_encoding):
        # Retrieve base record with matching key
        baseID = self.index.locate(record.key, self.key_index)
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
            [page_range_index, base_name_index, base_byte_pos] = self.page_directory[baseID]
            page_range = self.page_range_collection[page_range_index]
            tail_set = page_range.tail_set
            if len(tail_set) == 0: # Init State
                self.extend_tailSet(tail_set, first_rid=record.rid)
            else:
                tail_page_space = self.memory_manager.get_pages(tail_set[page_range.last_tail_name], table=self)[
                    INIT_COLS].has_space()
                self.memory_manager.unpinPages(tail_set[page_range.last_tail_name])
                if not tail_page_space:
                    # Current Tail Set does not have space
                    self.merge_queue.append(tail_set[page_range.last_tail_name])
                    self.extend_tailSet(tail_set, first_rid=record.rid)
                    page_range.last_tail_name += 1
            cur_tail_pages = self.memory_manager.get_pages(tail_set[page_range.last_tail_name], table=self, read_only=False)
            # Write to userdata columns
            self.write_to_pages(cur_tail_pages, record, schema_encoding, page_set_name=tail_set[page_range.last_tail_name], isUpdate=True)
            cur_base_pages = self.memory_manager.get_pages(page_range.base_set[base_name_index], table=self, read_only=False)

            # Read from base_set's indirection column
            base_indir_page = cur_base_pages[INDIRECTION_COLUMN]
            base_indir_data = self.convert_data(base_indir_page, base_byte_pos)

            # Make alias
            base_schema_page = cur_base_pages[SCHEMA_ENCODING_COLUMN]
            tail_schema_page = cur_tail_pages[SCHEMA_ENCODING_COLUMN]
            
            # Write to tail schema column (non-cumulative)
            tail_schema = ''
            for bit in schema_encoding:
                tail_schema += str(bit)
            schema_int = int(tail_schema)
            tail_schema_page.write(schema_int)
            # Merge tail & base schema
            [final_base_schema, _] = self.finalize_schema(base_schema_page, base_byte_pos)
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
            self.page_directory[record.rid] = [page_range_index, page_range.last_tail_name, byte_pos]
                   
            # Check if primary key is updated -- if it is then replace old key with new key 
            if record.columns[self.key_index] is not None:
                self.index.update_primaryKey(record.key, record.columns[self.key_index], self.key_index)

            if base_indir_data:  # Point to previous TID
                cur_tail_pages[INDIRECTION_COLUMN].write(base_indir_data)
            else:  # Point to baseID
                cur_tail_pages[INDIRECTION_COLUMN].write(baseID)
            # Base Indirection now points to current TID (replacement)
            base_indir_page.write(record.rid, base_byte_pos)

            # Both Base & Tail Pages modified Indirection and Schema Columns
            base_set_name = page_range.base_set[base_name_index]
            tail_set_name = page_range.tail_set[page_range.last_tail_name]
            self.memory_manager.setDirty(base_set_name)
            self.memory_manager.setDirty(tail_set_name)
            self.memory_manager.unpinPages(base_set_name)
            self.memory_manager.unpinPages(tail_set_name)
            """
            # NOTE previously.... (forgot to grab exlusive lock) ???
            self.memory_manager.isDirty[page_range.base_set[base_name_index]] = True
            self.memory_manager.isDirty[page_range.tail_set[page_range.last_tail_name]] = True
            self.memory_manager.pinScore[tail_set[page_range.last_tail_name]] -= 1
            self.memory_manager.pinScore[page_range.base_set[base_name_index]] -= 1
            """

    """
    # Given list of baseIDs, retrieves corresponding latest RIDs
    """
    def get_latest(self, baseIDs):
        rid_output = [] # List of RIDs

        for baseID in baseIDs:
            # Retrieve value in base record's indirection column
            page_range_index, base_name_index, base_byte_pos = self.page_directory[baseID]
            base_set = self.page_range_collection[page_range_index].base_set
            base_indir_page = self.memory_manager.get_pages(base_set[base_name_index], self)[INDIRECTION_COLUMN]
            latest_RID = self.convert_data(base_indir_page, base_byte_pos)
            if latest_RID == 0: # No updates made
                rid_output.append(baseID)
            else: # Base record has been updated
                rid_output.append(latest_RID)
            self.memory_manager.unpinPages(base_set[base_name_index])
        return rid_output

    
    """
    # Given a RID, retrieves corresponding previous RID, if any
    """
    def get_previous(self, rid):
        # Retrieve value in tail record's indirection column
        [page_range_index, name_index, byte_pos] = self.page_directory[rid]    
        page_range = self.page_range_collection[page_range_index]

        tail_set = page_range.tail_set      
        if len(tail_set) == 0 or rid < self.TID_counter: 
            # No updates made to page_range, or rid=baseID
            return 0
        else:
            tail_indir_page = self.memory_manager.get_pages(tail_set[name_index], table=self)[INDIRECTION_COLUMN]
            prev_RID = self.convert_data(tail_indir_page, byte_pos)
            self.memory_manager.unpinPages(tail_set[name_index])
            return prev_RID # Single RID


    """
    # Helper function to generate nicknames for given threadID (can remove later)
    """
    def gen_thread_nickname(self, curr_threadID):
        try:
            self.all_threads[curr_threadID]
        except KeyError:
            self.all_threads[curr_threadID] = chr(ord(self.thread_nickname) + self.thread_count)
            self.thread_count += 1 # To generate new nickname in advance
        return self.all_threads[curr_threadID] 


    """
    # Attempt to grab a Shared Lock for given RID
    """
    def get_shared_lock(self, rid, curr_threadID, latch, init_flag=True):
        # Alias for threadID
        thread_nickname = self.all_threads[curr_threadID]
        try:
            baseID = rid # Init value, rid may be a TID though
            if rid >= self.TID_counter:
                [page_range_index, name_index, tail_byte_pos] = self.page_directory[rid]
                tail_name = self.page_range_collection[page_range_index].tail_set[name_index]
                mapped_base_page = self.memory_manager.get_pages(tail_name, table=self)[BASE_RID_COLUMN]
                # Update bookkeeping
                self.memory_manager.unpinPages(tail_name)
                # Get mapped BaseID
                mapped_baseID = self.convert_data(mapped_base_page, tail_byte_pos)
                baseID = mapped_baseID

            # Locate mapped BaseID
            [page_range_index, base_name_index, base_byte_pos] = self.page_directory[baseID]

            # See if baseID exists and has no outstanding writers
            if not self.lock_manager.exclusive_locks[baseID].acquire(blocking=False):
                try:
                    baseIDs_to_tailIDs = self.lock_manager.threadID_to_tids[curr_threadID]
                    # Current thread created at least one Tail record
                    # updated BaseID <-- committed TID <-- first TID made <-- ...
                    for updated_baseID in list(baseIDs_to_tailIDs):
                        tids_made = baseIDs_to_tailIDs[updated_baseID]
                        first_tid_made = tids_made[0]
                        # Read Indirection of base record's first tail record
                        committed_TID = self.get_previous(first_tid_made)
                        # Locate updated baseID
                        [_, base_name_index, base_byte_pos] = self.page_directory[updated_baseID]
                        base_name = self.page_range_collection[page_range_index].base_set[base_name_index]
                        # Restore base record's committed indirection
                        base_indir_page = self.memory_manager.get_pages(base_name, table=self)[INDIRECTION_COLUMN]
                        base_indir_page.write(committed_TID, pos=base_byte_pos)
                        # Update bookkeeping
                        self.lock_manager.exclusive_locks[updated_baseID].acquire()
                        self.memory_manager.unpinPages(base_name)
                        self.lock_manager.exclusive_locks[updated_baseID].release()
                        # Rollback: Invalidate all TIDs made for updated baseID
                        self.invalid_rids += tids_made
                        # Release exlusive lock for updated baseID
                        self.lock_manager.exclusive_locks[updated_baseID].release()
                except KeyError:
                    # Current Thread hasn't updated any baseIDs (has only performed reads only)
                    # Nothing to rollback
                    pass
                latch.release()
                return False # Signal Transaction to abort()
            elif init_flag or rid >= self.TID_counter:
                # Avoid over-incrementing num_sharers for baseIDs
                self.lock_manager.shared_locks[baseID] += 1
        except KeyError:
            # First time using RID
            self.lock_manager.shared_locks[baseID] = 1
        #print("Thread {} finished incrementing RID={}\n".format(thread_nickname, baseID))
        

    """
    # Release all acquired Shared Locks acquired for given RIDs
    """
    def free_shared_locks(self, rids_accessed, thread_nickname):
        # Print stmts just for debugging purposes for now
        for rid in rids_accessed:
            #print("= = = = = " * 10)
            baseID_used = rid # Init Value
            if rid >= self.TID_counter:
                # Get mapped base record ID
                [page_range_index, tail_name_index, tail_byte_pos] = self.page_directory[rid]
                tail_name = self.page_range_collection[page_range_index].tail_set[tail_name_index]
                mapped_base_page = self.memory_manager.get_pages(tail_name, table=self)[BASE_RID_COLUMN]
                # Update bookkeeping
                self.memory_manager.unpinPages(tail_name)
                baseID_used = self.convert_data(mapped_base_page, tail_byte_pos)                

            init_sharers = deepcopy(self.lock_manager.shared_locks[baseID_used])
            self.lock_manager.shared_locks[baseID_used] -= 1
            """
            print("Thread {} will decrement for RID={}. \
                    BEFORE num sharers: {} vs AFTER num sharers: {}".format( \
                    thread_nickname, baseID_used, init_sharers, self.lock_manager.shared_locks[baseID_used]))
            print("= = = = = " * 10)
            """
        #print("-------One Read Operation completed-------------")


    """
    # Reads record(s) with matching key value and indexing column
    """
    def read_records(self, key, column, query_columns, max_key=None):
        baseIDs = 0      
        if max_key == None:
            result = self.index.locate(key, column)
            if result != INVALID_RECORD:
                baseIDs = [result] # Make into a list with one baseID
        else: # Performing multi reads for summation 
           baseIDs = self.index.locate_range(key, max_key, column)

        # Obtain latch for LockManager & current ThreadID
        #self.memory_manager.lock.acquire()
        latch = self.memory_manager.latches[self.name]
        #self.memory_manager.lock.release()

        curr_threadID = threading.get_ident()
        thread_nickname = self.gen_thread_nickname(curr_threadID)

        # Check if all baseIDs are available for reading
        latch.acquire()
        for baseID in baseIDs:
            self.get_shared_lock(baseID, curr_threadID, latch)
        # Default behavior if no abort
        latch.release()

        latest_rids = self.get_latest(baseIDs)
        output = [] # A list of Record objects to return
        # Track all RIDs accessed during read_records
        rids_accessed = latest_rids

        for rid in latest_rids:
            # Validation Stage: Check if rid is invalid
            latch.acquire() # (Aborted) Updates may happen concurrently
            if rid in self.invalid_rids:
                latch.release()
                continue # Go to next rid in latest_rids
            latch.release()
            
            # Check if all rids (previous and latest ones) are available for reading
            latch.acquire()
            # Avoid incrementing twice if rid == baseID, so init_flag=False
            self.get_shared_lock(rid, curr_threadID, latch, init_flag=False)
            # Default behavior if no abort
            latch.release()
            # Initialize data to base record's data
            data = [None] * self.num_columns
            columns_not_retrieved = set()

            # Determine columns not retrieved yet
            for i in range(len(query_columns)):
                if query_columns[i] == 1:
                    columns_not_retrieved.add(i)

            # Locate record within Page Range
            [page_range_index, name_index, byte_pos] = self.page_directory[rid]
            page_range = self.page_range_collection[page_range_index]
            if rid >= self.TID_counter:
                # Find corresponding Base Record for Tail Record
                tail_set_name = page_range.tail_set[name_index]
                tail_pages = self.memory_manager.get_pages(tail_set_name, table=self)
                mapped_baseID_page = tail_pages[BASE_RID_COLUMN]
                mapped_baseID = self.convert_data(mapped_baseID_page, byte_pos)
                self.memory_manager.unpinPages(tail_set_name)
                [_, base_name_index, base_byte_pos] = self.page_directory[mapped_baseID]
                base_set_name = page_range.base_set[base_name_index]
            else: # Current rid is a BaseID
                [_, base_name_index, base_byte_pos] = self.page_directory[rid]
                base_set_name = page_range.base_set[base_name_index]

            # Read Base Record's TPS
            base_pages = self.memory_manager.get_pages(base_set_name, table=self)

            for i in range(self.num_columns):
                data[i] = self.convert_data(base_pages[INIT_COLS + i], base_byte_pos)
            base_tps_page = base_pages[TPS_COLUMN]
            base_tps = self.convert_data(base_tps_page, base_byte_pos)

            # Read Base Record's Indirection RID
            base_indir_page = base_pages[INDIRECTION_COLUMN]
            base_indir_rid = self.convert_data(base_indir_page, base_byte_pos)
            self.memory_manager.unpinPages(base_set_name)

            # Retrieve whatever data you can from latest record
            while len(columns_not_retrieved) > 0:
                if rid == base_tps:
                    print('tps skip')
                    break
                # Locate record within Page Range
                [page_range_index, name_index, byte_pos] = self.page_directory[rid]
                page_range = self.page_range_collection[page_range_index]
                # Retrieve whatever data you can from latest record
                assert rid != 0
                # RID may be a base or a tail ID
                if rid >= self.TID_counter:
                    page_set = page_range.tail_set
                else: # Reading a Base Record
                    page_set = page_range.base_set

                # Read schema data
                buffer_page_set = self.memory_manager.get_pages(page_set[name_index], self)
                schema_page = buffer_page_set[SCHEMA_ENCODING_COLUMN]

                [schema_str, _] = self.finalize_schema(schema_page, byte_pos)
                self.memory_manager.unpinPages(page_set[name_index])
                # Leading zeros are lost after integer conversion, so padding needed
                if len(schema_str) < self.num_columns:
                    diff = self.num_columns - len(schema_str)
                    schema_str = '0' * diff + schema_str
                for col, page in enumerate(self.memory_manager.get_pages(page_set[name_index], table=self)[INIT_COLS:]):
                    if col not in columns_not_retrieved:
                        continue
                    # Retrieve values from older records, if they are not in the newest ones
                    if rid < self.TID_counter or bool(int(schema_str[col])):
                        data[col] = self.convert_data(page, byte_pos)
                        columns_not_retrieved.discard(col)
                self.memory_manager.unpinPages(page_set[name_index])
                # Get RID from indirection column
                prev_rid = self.get_previous(rid)
                if prev_rid < self.TID_counter:
                    break # Base record encountered
                else:   
                    rids_accessed += [prev_rid]
                    rid = prev_rid # Follow lineage

            ### End of while loop ###
            # Given key might not be a primary key
            primary_key = data[self.key_index]
            # Append each RID's record into a list
            record = Record(rid, primary_key, data)
            output.append(record)

        ### End of outer for loop ###
        # Release all Shared Locks for all RIDs accessed during query
        latch.acquire()
        self.free_shared_locks(rids_accessed, thread_nickname)
        latch.release()
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
        baseID = self.index.locate(key, self.key_index)
        [page_range_index, name_index, byte_pos] = self.page_directory[baseID]        

        # Invalidate base record
        page_range = self.page_range_collection[page_range_index]
        base_set_name = page_range.base_set[name_index]
        base_rid_page = self.memory_manager.get_pages(base_set_name, table=self, read_only=False)[RID_COLUMN]
        self.memory_manager.exclusiveLocks[base_set_name].acquire()
        base_rid_page.write(INVALID_RECORD, byte_pos)
        self.memory_manager.isDirty[base_set_name] = True
        self.memory_manager.pinScore[base_set_name] -= 1
        self.memory_manager.exclusiveLocks[base_set_name].release()

        # Invalidate all tail records, if any
        next_rid = self.get_latest([baseID])[0] # get_latest() returns a list
        num_deleted = 0

        # Add either baseID or latest tailID
        representative = next_rid if next_rid != 0 else baseID
        self.invalid_rids.append(representative)

        # Start from most recent tail records to older ones      
        while (next_rid != 0): # At least one tail record exists
            self.invalid_rids.append(next_rid)
            num_deleted += 1
            next_rid = self.get_previous(next_rid)

        # Update Indexer & Page Range Collection
        self.index.indices[self.key_index].pop(key)
        page_range.num_updates -= num_deleted


    """
    # Merges base & tail records within a Page Range
    """
    def __merge(self):
        # Continue merging while there are outdated Base Pages 
        while True:
            if len(self.merge_queue) > 0:
                batchTailPageName = self.merge_queue.pop(0)
                batchTailPage = self.memory_manager.get_pages(batchTailPageName, table=self)
                seenRIDS = set()
                for i in range(PAGE_CAPACITY):
                    tail_byte_pos = 8 * (PAGE_CAPACITY - 1 - i)
                    tail_rid = self.convert_data(batchTailPage[RID_COLUMN], tail_byte_pos)
                    if tail_rid in seenRIDS:
                        continue

                    base_rid = self.convert_data(batchTailPage[BASE_RID_COLUMN], tail_byte_pos)
                    # Get corresponding base page
                    [page_range_index, base_name_index, base_byte_pos] = self.page_directory[base_rid]
                    page_range = self.page_range_collection[page_range_index]
                    base_name = page_range.base_set[base_name_index]
                    batchConsPage = self.memory_manager.get_pages(base_name, self, merging=True)
                    data = [None] * self.num_columns
                    unseen_columns = set()
                    for j in range(self.num_columns):
                        unseen_columns.add(j)

                    cur_rid = tail_rid

                    while len(unseen_columns):
                        # Note that cur_name_index maps to different dictionaries depending on cur_rid
                        [page_range_index, cur_name_index, cur_byte_pos] = self.page_directory[cur_rid]
                        if cur_rid >= self.TID_counter:
                            page_set = self.page_range_collection[page_range_index].tail_set
                        else:
                            page_set = self.page_range_collection[page_range_index].base_set
                        batchCurPage = self.memory_manager.get_pages(page_set[cur_name_index], self)
                        # Get valid columns from schema
                        schema = self.finalize_schema(batchCurPage[SCHEMA_ENCODING_COLUMN], cur_byte_pos)[0]
                        for j in range(len(schema)):
                            if (int(schema[j]) or cur_rid < self.TID_counter) and j in unseen_columns:
                                data[j] = self.convert_data(batchCurPage[INIT_COLS + j], cur_byte_pos)
                                unseen_columns.discard(j)
                        cur_rid = self.convert_data(batchCurPage[INDIRECTION_COLUMN], cur_byte_pos)
                        seenRIDS.add(cur_rid)
                        self.memory_manager.unpinPages(page_set[cur_name_index])
                    # Write over data in base page copy
                    for j in range(INIT_COLS, INIT_COLS + self.num_columns):
                        batchConsPage[j].write(data[j-INIT_COLS], base_byte_pos)
                        self.convert_data(batchConsPage[j], base_byte_pos)
                    # Write over TPS column
                    batchConsPage[TPS_COLUMN].write(tail_rid, base_byte_pos)
                    # Write consolidated base page
                    self.memory_manager.get_pages(base_name, self)
                    self.memory_manager.exclusiveLocks[base_name].acquire()
                    self.memory_manager.bufferPool[base_name][TPS_COLUMN] = batchConsPage[TPS_COLUMN]
                    self.memory_manager.bufferPool[base_name][INIT_COLS:] = batchConsPage[INIT_COLS:]
                    self.memory_manager.exclusiveLocks[base_name].release()
                    self.memory_manager.unpinPages(base_name)
                    self.num_merged += 1
                self.memory_manager.unpinPages(batchTailPageName)
