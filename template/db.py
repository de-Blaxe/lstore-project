from template.table import Table

class BufferPool():
    def __init__(self):
        # map rid to associated pages
        self.pages = dict()
        # map rid to dirty bit
        self.isDirty = dict()
        # map rid to eviction score
        self.evictionScore = dict()
        # leastUsedRecord is some rid
        self.leastUsedRecord = 0

    def get_page(self, rid):
        if rid not in self.pages:
            self.replace_pages(rid)
        return self.pages[rid]

    def replace_pages(self, rid):
        # assuming eviction policy is LRU
        pass
        


class Database():
    def __init__(self):
        self.tables = dict() # Index tables by their unique names
        self.bufferpool = BufferPool()
        pass

    def open(self, path):
        """
        # Store one file per table?
        # Assuming path to file is the same as table name?
        """
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
