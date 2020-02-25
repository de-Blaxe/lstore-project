from template.table import Table
import os
from template.config import *
from template.page import Page

class MemoryManager():
    def __init__(self, db_path):
        # NOTE: all pages belonging to a set should share the same name
        self.db_path = db_path
        # map pageSetName to associated pageSet
        self.bufferPool = dict()
        # map pageSetName to dirty bit
        self.isDirty = dict()
        # map pageSetName to pins
        self.pinScore = dict()
        # map pageSetName to eviction score
        self.evictionScore = dict()
        # leastUsedPage is a pageSetName
        self.leastUsedPageSet = ""
        self.maxSets = 2

    def get_pages(self, page_set_name, table):
        if page_set_name not in self.bufferPool:
            self._replace_pages(page_set_name, table)
        self._increment_scores(retrieved_page_set_name=page_set_name)
        return self.bufferPool[page_set_name]

    def create_page_set(self, page_set_name, table):
        if len(self.bufferPool) > self.maxSets:
            # assuming eviction policy is LRU
            if self.isDirty[self.leastUsedPageSet]:
                self._write_set_to_disk(page_set_name, table)
            self.bufferPool.pop(self.leastUsedPageSet, None)
        cur_set = []
        for _ in range(INIT_COLS + table.num_columns):
            cur_set.append(Page())
        self.bufferPool[page_set_name] = cur_set
        self.isDirty[page_set_name] = True
        self.pinScore[page_set_name] = 0
        self._increment_scores(page_set_name)

    def _replace_pages(self, page_set_name, table):
        if len(self.bufferPool) > self.maxSets:
            # assuming eviction policy is LRU
            if self.isDirty[self.leastUsedPageSet]:
                self._write_set_to_disk(page_set_name, table)
            self.bufferPool.pop(self.leastUsedPageSet, None)
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

    def _write_set_to_disk(self, page_set_name, table):
        self._navigate_table_directory(table.name)
        with open(page_set_name, 'wb') as file:
            for i in range(table.num_columns + INIT_COLS):
                cur_page = self.bufferPool[page_set_name][i]
                file.write(cur_page.num_records.to_bytes(8, 'little'))
                file.write(cur_page.first_unused_byte.to_bytes(8, 'little'))
                file.write(cur_page.data)

    def _increment_scores(self, retrieved_page_set_name):
        max_score = self.evictionScore.get(retrieved_page_set_name, 0)
        for pageSetName, score in self.evictionScore.items():
            if score <= max_score:
                self.evictionScore[pageSetName] += 1
            if score > self.evictionScore.get(self.leastUsedPageSet, -1):
                self.leastUsedPageSet = pageSetName
                self.evictionScore[self.leastUsedPageSet] = score
        self.evictionScore[retrieved_page_set_name] = 0

    def _navigate_table_directory(self, table_name):
        print(os.getcwd())
        os.chdir(self.db_path)
        try:
            os.chdir(os.path.join(self.db_path, table_name))
        except:
            os.mkdir(os.path.join(self.db_path, table_name))
            os.chdir(os.path.join(self.db_path, table_name))


class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names

        pass

    def open(self, path):
        """
        # Store one file per table?
        # Assuming path to file is the same as table name?
        """
        self.memory_manager = MemoryManager(path)
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
            # Iterate through pages and invalidate RID of all records
            """
            page_dir = table_match.page_directory
            rids = page_dir.keys()
            for rid in rids:
                # account for page ranges
            """
            pass
