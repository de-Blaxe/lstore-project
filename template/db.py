from template.table import Table
import os
from template.config import *

class MemoryManager():
    def __init__(self, dbPath):
        self.dbPath = dbPath
        # map rid to associated pages
        self.bufferPool = dict()
        # map rid to dirty bit
        self.isDirty = dict()
        # map rid to eviction score
        self.evictionScore = dict()
        # leastUsedRecord is some rid
        self.leastUsedRecord = 0

    def get_records(self, rid, table):
        if rid not in self.pages:
            self.replace_pages(rid, table)
        self.incrementScores(rid)
        return self.pages[rid]

    def replace_pages(self, rid, table):
        # assuming eviction policy is LRU
        if self.isDirty[self.leastUsedRecord] == True:
            self.write_page_disk(self, rid)
        self.pages.pop(self.leastUsedRecord, None)
        # find page
        try:
            os.chdir(self.dbPath + table.name)
        except:
            os.mkdir(self.dbPath + table.name)
            os.chdir(self.dbPath + table.name)
        # write file
        page_name = table.page_directory[rid][3]
        with open(page_name, 'r') as file:
            # overhead is 16 bytes
            # page size is defined
            page_set = []
            for i in range(table.num_columns + INIT_COLS):
                page =





    def incrementScores(self, recent_rid):
        max_score = self.evictionScore[recent_rid]
        for rid, score in self.evictionScore.items:
            if score <= max_score:
                self.evictionScore[rid] += 1
            if score >= self.evictionScore[self.leastUsedRecord]:
                self.leastUsedRecord = rid
        self.evictionScore[recent_rid] = 0


class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names

        pass

    def open(self, path):
        """
        # Store one file per table?
        # Assuming path to file is the same as table name?
        """
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
        names = self.tables.keys()
        if name in names:
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
