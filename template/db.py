from template.table import Table

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self):
        pass

    def close(self):
        pass

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key_index: int             #Index of table key in columns
    """
    def create_table(self, name, key_index, num_columns):
        table = Table(name, key_index, num_columns)
        # self.tables.apppend(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        ### Find table in self.tables
        ### Iterate through pages and invalidate RID of all records
        pass
