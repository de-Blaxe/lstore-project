from template.table import Table
import os
from template.config import *
from template.page import Page

import operator

class MemoryManager():
    def __init__(self, db_path):
        # NOTE: all pages belonging to a set should share the same
        self.db_path = db_path
        # map rid to associated pageSet
        self.bufferPool = dict()
        # map pageSetName to dirty bit
        self.isDirty = dict()
        # map pageSetName to eviction score
        self.evictionScore = dict()
        # leastUsedPage is a pageSetName
        self.leastUsedPageSet = ""
        

    """
    def _find_lru_items(self):
        # Retrieve single pageSet(Name) with highest eviction score
        elected_pageSet = max(self.evictionScore.keys(), key=operator.itemgetter(1))[0] 
        self.leastUsedPageSet = elected_pageSet
        # Retrieve least recently used RID
        pass
        # IncrementScores() later resets leastUsedPageSet's eviction score to zero
    """

    """
    def _has_space(self):
        return len(self.bufferPool) <= BUFFER_SIZE
    """

    def _get_records(self, rid, table):
        # Update least used pageSet & record
        # self._find_lru_items()
        # Need to check if bufferpool has space before evicting
        if rid not in self.bufferPool:
            self._replace_pages(rid, table)
        self._incrementScores(page_name = table.page_directory[rid][3])
        return self.bufferPool[rid]

    def _replace_pages(self, rid, table):
        # assuming eviction policy is LRU
        if self.isDirty[self.leastUsedPageSet]:
            self._write_set_to_disk(rid, table)
        self.bufferPool.pop(self.leastUsedRecord, None)
        # find page
        try:
            os.chdir(self.db_path + '/' + table.name)
        except:
            os.mkdir(self.db_path + '/' + table.name)
            os.chdir(self.db_path + '/' + table.name)
        # read file
        page_name = table.page_directory[rid][3]
        with open(page_name, 'rb') as file:
            # overhead is 16 bytes
            page_set = []
            for i in range(table.num_columns + INIT_COLS):
                unpacked_num_records = int.from_bytes(file.read(8), 'little')
                unpacked_first_unused_byte = int.from_bytes(file.read(8), 'little')
                unpacked_data = bytearray(file.read(PAGE_SIZE))
                page_set.append(Page(unpacked_num_records, unpacked_first_unused_byte, unpacked_data))
        self.bufferPool[rid] = page_set
        os.chdir(self.db_path)

    def _write_set_to_disk(self, rid, table):
        os.chdir(self.db_path)

    def _incrementScores(self, page_set_name):
        max_score = self.evictionScore[page_set_name]
        for rid, score in self.evictionScore.items:
            if score <= max_score:
                self.evictionScore[rid] += 1
            if score >= self.evictionScore[self.leastUsedPageSet]: # fixed typo
                self.leastUsedRecord = rid
        self.evictionScore[page_set_name] = 0


class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names
        pass

    def open(self, path):
        self.bufferpool = MemoryManager(path)
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_index: int       #Index of table key in columns
    """
    def create_table(self, name, key_index, num_columns):
        # Base case: Check for duplicate table names
        if name in self.tables:
           print("Error: Duplicate Table Name\n")
           return None # Should we exit() instead?

        table = Table(name, key_index, num_columns)
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
            # Iterate through pages and invalidate RID of all records
            """
            page_dir = table_match.page_directory
            rids = page_dir.keys()
            for rid in rids:
                # account for page ranges
            """
            pass
