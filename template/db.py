from template.table import Table

class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names
        pass

    def open(self, path): # Merged template code; added parameter "path"
        # Path to file (disk)?
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
                page_set = page_dir[rid]
                for page in page_set:
                   if rid >= table_match.TID_counter: # Tail recordID
                       byte_pos = abs(rid - (2 ** 64 - 1)) % PAGE_CAPACITY
                   else:
                       byte_pos = (rid - 1) % PAGE_CAPACITY
                   # Note: INVALID_RECORD defined as -1 in config
                   page_set[page].write(INVALID_RECORD, byte_pos)
            """
            pass
