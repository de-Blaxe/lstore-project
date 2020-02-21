from template.config import *

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        #self.indices = [dict()] * table.num_columns # BUG!
        self.primary_index = table.key_index
        self.indices = [dict() for _ in range(table.num_columns)] # corrected        

    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        print("Locate(): key val=", key_val, " and column=", column)
        print("Locate(): all keys inside dictionary[column]: ", self.indices[column].keys())        

        if key_val not in self.indices[column].keys():
            raise KeyError
        # Return baseID(s) associated with key_val based on column_number
        # Dictionaries for primary keys have a 1:1 mapping with their baseIDs
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
    def update_index(self, key, new_key, cur_rid, column_number): # added cur_rid (the current value stored for record)
        """
        # Search and replace dictionary entry
        base_rids = self.locate(key, column_number)
        self.indices[column_number][new_key] = base_rids # BUG This will map new_key to ALL base_rids, not just one
        # Delete old key in dictionary
        self.indices[column_number].pop(key)
        """
        # Check if new_key is unique
        try:
            self.indices[column_number][new_key]
        except KeyError:
            self.indices[column_number][new_key] = []
        self.indices[column_number][new_key].append(cur_rid)

        # Delete cur_rid from previous mapping
        cur_values = self.indices[column_number].values()
        if len(cur_values) > 1:
            # There are other baseIDs mapped to old key
            self.indices[column_number][key].remove(cur_rid)
        else: # Safe to remove old key completely
            self.indices[column_number].pop(key)


    """
    Create index on specific column
    """
    def create_index(self, key, val, column_number):
        # Dictionary Entry: {col value : baseID}
        ## Previous Idea##
        # Alternative: self.indices[column_number].update({key:val})
        #self.indices[column_number][key] = val

        ## New Idea ##
        # Dictionary Entry: {col val : list of all matching baseIDs}
        # But then you need to access stuff differently in table.py
        # Note: Sum queries only called on primary key
    
        if column_number == self.primary_index:
            self.indices[column_number][key] = val
        else:
            try:
                self.indices[column_number][key]
            except KeyError: # No existing mapping yet
                # Initialize to empty list, once per non-primary column
                self.indices[column_number][key] = []
            self.indices[column_number][key].append(val)

   
    """
    Drop index of specific column
    """
    def drop_index(self, table, column_number):
        # Base case: Check if column_number in range
        if column_number < 0 or column_number > len(self.indices):
            print("Drop Index Error: column_number is out of range.\n")
            return
        # Remove index on specific column
        # self.indices = self.indices[column_number + 1:] # before, this shortens indices length
        # I think you need to have empty placeholder so column_number/indexing doesn't get messed up
        self.indices[column_number] = {} # Empty dictionary placeholder
