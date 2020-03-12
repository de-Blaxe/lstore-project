from template.table import Table
import os
from template.config import *
from template.page import Page
from copy import deepcopy
import pickle # Used to save Table instances
import threading
from collections import defaultdict

class LockManager:

    def __init__(self):
        # Maps ThreadIDs to TailIDs created during threading (in case of Rollback -> mark those TIDs as invalid)
        self.threadID_to_tids = defaultdict(list)
        # Maps BaseIDS to number of Shared Locks (0+: Available [read], -1: Exclusive Lock [write])
        self.shared_locks = dict()
        # Maps BaseIDs to actual RLock
        self.exclusive_locks = dict()

class MemoryManager():

    def __init__(self, path):
        # Store db path for later Table navigation
        self.db_path = path
        # Map pageSetName to associated pageSet
        self.bufferPool = dict()
        # Map pageSetName to dirty bit
        self.isDirty = dict()
        # Map pageSetName to pins
        self.pinScore = dict()
        # Counter, incremented by _evict()
        self.num_evicted = 0
        # Index -> PageSetName. Index represents evictionScore -> LRU policy
        # Newly created PageSets put at front (lower eviction score)
        self.evictionQueue = []
        # LeastUsedPage is a pageSetName
        self.leastUsedPageSet = ""
        self.maxSets = 10
        # Protect MemoryManager bookkeeping
        self.exclusiveLocks = dict()
        self.evictionLock = threading.RLock()
        # Create a unique latch for each Table's LockManager
        # Each latch protects a LockManager
        self.latches = dict()


    """
    # Creates a latch for each Table's Lock Manager.
    """
    def create_latch(self, table_name):
        self.latches[table_name] = threading.RLock() # Use RLock instead of Lock?


    """
    # Milestone 3 idea - get_pages()
    def get_pages(self, page_set_name, table, read_only=True):
        # Dont pin if we know we're strictly reading
        if not read_only:
            self.pinScore[page_set_name] = self.pinScore.get(page_set_name, 0) + 1
        # <insert rest of get_pages() body here>
    """

    """
    # Retrieves requested Page Set from either Disk or BufferPool.
    # Returns copy if merging; else, actual pointer.
    """
    def get_pages(self, page_set_name, table, merging=False, read_only=True):
        if merging:
            try:
                page_set = deepcopy(self.bufferPool[page_set_name])
            except KeyError:
                with open(os.path.join(self.db_path, table.name, page_set_name), 'rb') as file:
                    # Overhead is 16 bytes
                    page_set = []
                    for i in range(table.num_columns + INIT_COLS):
                        unpacked_num_records = int.from_bytes(file.read(8), 'little')
                        unpacked_first_unused_byte = int.from_bytes(file.read(8), 'little')
                        unpacked_data = bytearray(file.read(PAGE_SIZE))
                        page_set.append(Page(unpacked_num_records, unpacked_first_unused_byte, unpacked_data))
            return page_set
        # Do not evict
        self.evictionLock.acquire()
        self.exclusiveLocks[page_set_name] = self.exclusiveLocks.get(page_set_name, threading.RLock())
        self.exclusiveLocks[page_set_name].acquire()
        self.pinPages(page_set_name)
        if page_set_name not in self.bufferPool:
            self._replace_pages(page_set_name, table)
        self._increment_scores(retrieved_page_set_name=page_set_name)
        try:
            self.exclusiveLocks[page_set_name].release()
        except KeyError:
            # This should raise a keyerror if the page_set was evicted because it gets removed from the dict
            pass
        self.evictionLock.release()
        return self.bufferPool[page_set_name]


    """
    # Creates a Page Set & inserts it into BufferPool.
    """
    def create_page_set(self, page_set_name, table):
        self.exclusiveLocks[page_set_name] = self.exclusiveLocks.get(page_set_name, threading.RLock())
        self.exclusiveLocks[page_set_name].acquire()
        if page_set_name not in self.bufferPool and not os.path.exists(os.path.join(self.db_path, table.name, page_set_name)):
            self._evict(table)
            cur_set = [] # List of Pages (one per column)
            for _ in range(INIT_COLS + table.num_columns):
                cur_set.append(Page())
            self.bufferPool[page_set_name] = cur_set
            self.isDirty[page_set_name] = True
            self.pinScore[page_set_name] = self.pinScore.get(page_set_name, 0)
            self.evictionLock.acquire()
            # Place recently created page sets at the front of list
            self.evictionQueue.insert(0, page_set_name)
            self.evictionLock.release()
        self.exclusiveLocks[page_set_name].release()


    """
    # Pin specified pageSet
    """
    def pinPages(self, page_set_name):
        self.exclusiveLocks[page_set_name].acquire()
        self.pinScore[page_set_name] = self.pinScore.get(page_set_name, 0) + 1
        self.exclusiveLocks[page_set_name].release()


    """
    # Unpin specified pageSet
    """
    def unpinPages(self, page_set_name):
        self.exclusiveLocks[page_set_name].acquire()
        self.pinScore[page_set_name] -= 1
        self.exclusiveLocks[page_set_name].release()


    """
    # Set Dirty bit for pageSet
    """
    def setDirty(self, page_set_name):
        self.exclusiveLocks[page_set_name].acquire()
        self.isDirty[page_set_name] = True
        self.exclusiveLocks[page_set_name].release()

    """
    # Acquire Exclusive Lock for pageSet
    """
    def lockPages(self, page_set_name):
        self.exclusiveLocks[page_set_name].acquire()

    """
    # Release Exclusive Lock for pageSet
    """
    def unlockPages(self, page_set_name):
        self.exclusiveLocks[page_set_name].release()


    """
    # Evicts LRU Page Set in BufferPool.
    # Retrieves requested Page Set from Disk & inserts it into BufferPool.
    """
    def _replace_pages(self, page_set_name, table):
        # Read file
        if type(page_set_name) is not str:
            raise Exception
        with open(os.path.join(self.db_path, table.name, page_set_name), 'rb') as file:
            # Overhead is 16 bytes
            page_set = []
            for i in range(table.num_columns + INIT_COLS):
                unpacked_num_records = int.from_bytes(file.read(8), 'little')
                unpacked_first_unused_byte = int.from_bytes(file.read(8), 'little')
                unpacked_data = bytearray(file.read(PAGE_SIZE))
                page_set.append(Page(unpacked_num_records, unpacked_first_unused_byte, unpacked_data))
        self.evictionLock.acquire()
        self._evict(table)
        self.bufferPool[page_set_name] = page_set
        self.evictionLock.release()
        if len(self.bufferPool) > self.maxSets:
            raise Exception
        self.isDirty[page_set_name] = False
        self.pinScore[page_set_name] = self.pinScore.get(page_set_name, 0)
        self.evictionLock.acquire()
        self.evictionQueue.insert(0, page_set_name)
        self.evictionLock.release()


    """
    # Writes given Page Set to Disk
    """
    def _write_set_to_disk(self, page_set_name, table):
        with open(os.path.join(self.db_path, table.name, page_set_name), 'wb') as file:
            for i in range(table.num_columns + INIT_COLS):
                cur_page = self.bufferPool[page_set_name][i]
                file.write(cur_page.num_records.to_bytes(8, 'little'))
                file.write(cur_page.first_unused_byte.to_bytes(8, 'little'))
                file.write(cur_page.data)


    """
    # Maintain bookkeeping for given Page Set (Name)
    """
    def _increment_scores(self, retrieved_page_set_name):
        self.evictionLock.acquire()
        max_score = self.evictionQueue.index(retrieved_page_set_name)
        # Reset retrieved page set's evictionScore to 0
        self.evictionQueue = self.evictionQueue[:max_score] + self.evictionQueue[max_score + 1:]
        self.evictionQueue.insert(0, retrieved_page_set_name)
        self.evictionLock.release()


    """
    # Evicts LRU Page Set in BufferPool. Writebacks to Disk, if needed.
    """
    def _evict(self, table):
        self.evictionLock.acquire()
        if len(self.bufferPool) == self.maxSets:
            # Assuming eviction policy is LRU
            i = -1  # Start at the end of list
            # Find first unpinned Page Set
            try:
                while self.pinScore[self.evictionQueue[i]] != 0:
                    i -= 1
            except IndexError:
                print("bufferPool maxsets is too small to accomodate workload/threads.")
                print(i)
                raise IndexError
            evicting_page_set = self.evictionQueue[i]
            self.exclusiveLocks[evicting_page_set].acquire()
            while self.pinScore[evicting_page_set] != 0:
                pass
            if self.isDirty[evicting_page_set]:
                self._write_set_to_disk(evicting_page_set, table)
            self.evictionQueue.pop(i)
            self.isDirty.pop(evicting_page_set, None)
            self.pinScore.pop(evicting_page_set, None)
            self.bufferPool.pop(evicting_page_set, None)
            # Release lock
            self.exclusiveLocks.pop(evicting_page_set)
        self.evictionLock.release()


class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names


    """
    # Opens / Creates Disk for Database
    """
    def open(self, path):
        # Init single Memory Manager for Database
        self.db_path = os.path.expanduser(path)
        self.memory_manager = MemoryManager(self.db_path)
        try:
            os.chdir(self.db_path)
        except:
            os.mkdir(self.db_path)
            os.chdir(self.db_path)


    """
    # First, writebacks Dirty Page Sets, if any, to Disk. 
    # Then, saves Database and its Tables' bookkeeping info.
    """
    def close(self):
        # NOTE: THIS DOES NOT CONSIDER PINNED PAGES
        # Check if any remaining Dirty Pages in bufferpool
        for page_set_name, dirtyBit in self.memory_manager.isDirty.items():
            self.memory_manager.exclusiveLocks[page_set_name].acquire()
            if dirtyBit:
                # Write them back to disk
                [_, mapped_table_name] = page_set_name.split('_')
                self.memory_manager._write_set_to_disk(page_set_name, self.tables[mapped_table_name])

        # Need to save the tables dict to a file in order to retrive the table instance in get_table()
        for table in self.tables.values():
            table.memory_manager = None
            # Save Database & Table's bookkeeping dictionaries & indices
            with open(os.path.join(self.db_path, table.name, table.name) +'.pkl', 'wb') as output:
                pickle.dump(table, output, pickle.HIGHEST_PROTOCOL)
            with open(os.path.join(self.db_path, table.name, table.name) +'_index.pkl', 'wb') as output:
                pickle.dump(table.index, output, pickle.HIGHEST_PROTOCOL)
            with open(os.path.join(self.db_path, table.name, table.name) +'_page_directory.pkl', 'wb') as output:
                pickle.dump(table.page_directory, output, pickle.HIGHEST_PROTOCOL)
            with open(os.path.join(self.db_path, table.name, table.name) +'_update_to_pg_range.pkl', 'wb') as output:
                pickle.dump(table.update_to_pg_range, output, pickle.HIGHEST_PROTOCOL)


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_index: int       #Index of table key in columns
    """
    def create_table(self, name, key_index, num_columns):
        # Base case: Check for duplicate table names
        names = self.tables.keys()
        if name in names:
           print("Error: Duplicate Table Name\n")
           return None # Should we exit() instead?

        # Memory Manager shared by all Tables
        # Each Table gets exclusive Lock Manager
        lock_manager = LockManager()
        self.memory_manager.create_latch(name)       

        table = Table(name, key_index, num_columns, self.memory_manager, lock_manager)
        self.tables[name] = table
        try:
            os.mkdir(os.path.join(self.db_path, name))
        except:
            pass
        return table


    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        # Find table in self.tables
        try:
            table_match = self.tables[name]
        except KeyError:
            print("Error: ", name, " is not a valid Table\n")
            return 
        else:
            # Delete its file(s) from Disk: One directory per Table
            full_path = os.path.join(self.db_path, name)
            # Get list of all files in Table
            files_to_remove = os.listdir(full_path)
            for file_name in files_to_remove:
                os.remove(file_name)
            # Now, remove the emptied directory
            os.rmdir(full_path)
            # Alias
            mem = self.memory_manager
            # Iterate thru bufferpool dictionary
            for encoding in mem.bufferpool:
                # Find pageSet names that contain 'name'
                [_, mapped_table_name] = encoding.split('_')
                if mapped_table_name == name:
                    mem.bufferpool.pop(encoding, None)
                    mem.isDirty.pop(encoding, None)
                    mem.evictionQueue.remove(encoding)
                    mem.pinScore.pop(encoding, None)
            # Delete from Database
            self.tables.pop(name)


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        # Load previously saved data for matching Table 
        with open(os.path.join(self.db_path, name, name) + '.pkl', 'rb') as input:
            self.tables[name] = pickle.load(input)
        with open(os.path.join(self.db_path, name, name) + '_index.pkl', 'rb') as input:
            self.tables[name].index = pickle.load(input)
        with open(os.path.join(self.db_path, name, name) + '_page_directory.pkl', 'rb') as input:
            self.tables[name].page_directory = pickle.load(input)
        with open(os.path.join(self.db_path, name, name) + '_update_to_pg_range.pkl', 'rb') as input:
            self.tables[name].update_to_pg_range = pickle.load(input)
        self.tables[name].memory_manager = self.memory_manager
        # Now look for the table_name and return the instance
        return self.tables[name]
