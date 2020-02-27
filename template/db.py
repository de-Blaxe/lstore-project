from template.table import Table
import os
from template.config import *
from template.page import Page


class MemoryManager():
    def __init__(self, path):
        # Store db path for later Table navigation
        self.db_path = path
        # map pageSetName to associated pageSet
        self.bufferPool = dict()
        # map pageSetName to dirty bit
        self.isDirty = dict()
        # map pageSetName to pins
        self.pinScore = dict()
        # index -> PageSetName. Index represents evictionScore -> LRU policy
        # Newly created PageSets put at front (lower eviction score)
        self.evictionScore = [] # [LOW:'0/Grades', ..., '99/Student':HIGH]
        # leastUsedPage is a pageSetName
        self.leastUsedPageSet = ""
        self.maxSets = 2


    def get_pages(self, page_set_name, table):
        if page_set_name not in self.bufferPool:
            self._replace_pages(page_set_name, table)
        self._increment_scores(retrieved_page_set_name=page_set_name)
        return self.bufferPool[page_set_name]


    def create_page_set(self, page_set_name, table):
        self._evict(table)
        cur_set = [] # List of Pages (one per column)
        for _ in range(INIT_COLS + table.num_columns):
            cur_set.append(Page())
        self.bufferPool[page_set_name] = cur_set
        self.isDirty[page_set_name] = True
        self.pinScore[page_set_name] = 0
        # Place recently created page sets at the front of list
        self.evictionScore.insert(0, page_set_name)


    def _replace_pages(self, page_set_name, table):
        self._evict(table)
        # find page
        self._navigate_table_directory(table.name)
        # read file
        with open(page_set_name, 'rb') as file:
            # overhead is 16 bytes
            page_set = []
            for i in range(table.num_columns + INIT_COLS):
                unpacked_num_records = int.from_bytes(file.read(8), 'little')
                unpacked_first_unused_byte = int.from_bytes(file.read(8), 'little')
                unpacked_data = bytearray(file.read(PAGE_SIZE))
                page_set.append(Page(unpacked_num_records, unpacked_first_unused_byte, unpacked_data))
        self.bufferPool[page_set_name] = page_set
        self.isDirty[page_set_name] = False
        self.pinScore[page_set_name] = 0
        self.evictionScore.insert(0, page_set_name)


    def _write_set_to_disk(self, page_set_name, table):
        self._navigate_table_directory(table.name)
        with open(page_set_name, 'wb') as file:
            for i in range(table.num_columns + INIT_COLS):
                cur_page = self.bufferPool[page_set_name][i]
                file.write(cur_page.num_records.to_bytes(8, 'little'))
                file.write(cur_page.first_unused_byte.to_bytes(8, 'little'))
                file.write(cur_page.data)


    def _increment_scores(self, retrieved_page_set_name):
        max_score = self.evictionScore.index(retrieved_page_set_name)
        # Reset retrieved page set's evictionScore to 0
        self.evictionScore = self.evictionScore[:max_score] + self.evictionScore[max_score + 1:]
        self.evictionScore.insert(0, retrieved_page_set_name)


    def _navigate_table_directory(self, table_name):
        try:
            os.chdir(os.path.join(self.db_path, table_name))
        except:
            os.mkdir(os.path.join(self.db_path, table_name))
            os.chdir(os.path.join(self.db_path, table_name))


    def _evict(self, table):
        if len(self.bufferPool) == self.maxSets:
            # assuming eviction policy is LRU
            i = -1 # Start at the end of list
            # Find first unpinned Page Set
            while self.pinScore[self.evictionScore[i]] != 0:
                i -= 1
            evicting_page_set = self.evictionScore[i]
            if self.isDirty[evicting_page_set]:
                self._write_set_to_disk(evicting_page_set, table)
            self.bufferPool.pop(evicting_page_set, None)
            self.evictionScore.pop(i)
            self.isDirty.pop(evicting_page_set, None)
            self.pinScore.pop(evicting_page_set, None)
        elif len(self.bufferPool) > self.maxSets:
            raise Exception


class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names
        pass


    def open(self, path):
        # Init single Memory Manager for Database
        self.db_path = os.path.expanduser(path)
        self.memory_manager = MemoryManager(self.db_path)
        try:
            os.chdir(self.db_path)
        except:
            os.mkdir(self.db_path)
            os.chdir(self.db_path)


    def close(self):
        # NOTE: THIS DOES NOT CONSIDER PINNED PAGES
        # Check if any remaining Dirty Pages in bufferpool
        for page_set_name, dirtyBit in self.memory_manager.isDirty.items():
            if dirtyBit:
                # Write them back to disk
                [_, mapped_table_name] = page_set_name.split('_')
                self.memory_manager._write_set_to_disk(page_set_name, self.tables[mapped_table_name])
        pass


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
        table = Table(name, key_index, num_columns, self.memory_manager)
        self.tables[name] = table
        
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
            return # Should we exit() instead?
        else:
            # Delete its file from Disk: One file per Table
            full_path = os.path.join(self.db_path, name)
            files_to_remove = os.listdir(full_path) # List of all files in Table
            for file_name in files_to_remove:
                os.remove(file_name)
            os.rmdir(full_path) # Remove emptied directory
        
            # Alias
            mem = self.memory_manager
            # Iterate thru bufferpool dictionary
            for encoding in mem.bufferpool:
                # Find pageSet names that contain 'name'
                [_, mapped_table_name] = encoding.split('_')
                if mapped_table_name == name:
                    mem.bufferpool.pop(encoding, None)
                    mem.isDirty.pop(encoding, None)
                    mem.evictionScore.remove(encoding)
                    mem.pinScore.pop(encoding, None)

            # Delete from Database
            self.tables.pop(name)


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        self.memory_manager._navigate_table_directory(name)
        # return self.tables[name] 
        # Above doesn't work since dictionary made by part 1, which gets erased when program exits -> m2 part 2 problem???
        pass 
