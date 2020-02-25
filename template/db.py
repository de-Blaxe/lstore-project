from template.table import Table
import os
from template.config import *
from template.page import Page

import operator
import threading

# NOTES
# THIS DB.PY VERSION DOES NOT DEAL WITH BUFFERPOOL MANAGEMENT YET
# MERGE THREAD IS NOT CALLED YET. 

"""
class MergeThread(threading.Thread):
    
    def __init__(self, table):
        threading.Thread.__init__(self)
        self.table = table
        thread = threading.Thread(target=__merge, args=[self])
        thread.setDaemon(False)
        thread.start()
        #thread = threading.Thread(target=self.run, args=())
        #thread.daemon = False
        #thread.start()

    def run(self, table):
        while True:
            table.merge()
"""

class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names
        pass

    def open(self, path):
        #self.bufferpool = MemoryManager(path)
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

        # Initiate Background Merging Thread
        #merge_thread = MergeThread(table)
        #merge_thread.start()

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
