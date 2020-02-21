from template.table import Table

class Database():

    def __init__(self):
        self.tables = dict() # Index tables by their unique names
        pass

    def open(self, path): pass
    """
    open and read to pages
    # Store one file per table?
    # Assuming path to file is the same as table name?
    Location: Current working directory
    DAN:: IDEA 1 (Folder:TABLE --> Folders:COLUMNS --> Pages:BASE and TAIL Records and data)
    DISK:: Folders (named after their index) for every column
    pages(named after the last RID and TID used (inherently sorted)) go into the columns folder
    - Base and tail pages that are put on disk are full pages.
    - Simple FULL Read (and write) procedure implementation
    - would possibly give us the ability to pull pages from individual columns for sum
    - index would work as normal and we would simply read in the pages when we know where the records are located to alias?
    MEM:
    Most current tail pages and base pages
    SELECT: would read in pages [all nine] that have record we need
    UPDATE: not quite sure what to do honestly
    SUM: not quite sure what to do honestly
    IDEA 2
    DISK:: one file for every column
    MEM::
    """

    def close(self):
        pass

    """
     MIGHT BE WORTH IMPLEMENTING MERGE FIRST before writing pages to disk.
        - Might need to build merge first
    #open and write to pages here
    """


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
