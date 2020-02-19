from template.config import *

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        #self.indices = [dict()] * table.num_columns # BUG! WRONG
        self.indices = [dict() for _ in range(table.num_columns)] # corrected        

    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        if key_val not in self.indices[column].keys():
            raise KeyError
        # Return value associated with key_val based on column_number
        return self.indices[column][key_val]


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column): 
        # TODO
        pass

    """
    # Updates dictionary for key replacement
    """
    def update_index(self, key, new_key, column_number):
        # Search and replace dictionary entry
        base_rid = self.locate(key, column_number)
        self.indices[column_number][new_key] = base_rid
        # Delete old key in dictionary
        self.indices[column_number].pop(key)


    """
    Create index on specific column
    """
    def create_index(self, key, val, column_number):
        # Dictionary Entry: {col value : baseID}
        # Alternative: self.indices[column_number].update({key:val})
        self.indices[column_number][key] = val
        
   
    """
    Drop index of specific column
    """
    def drop_index(self, table, column_number):
        # Remove index on specific column
        self.indices = self.indices[column_number + 1:]
