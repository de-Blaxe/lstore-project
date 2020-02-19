from template.config import *

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

#TODO: Dealing with non unique key values for the same indexed column
#Example
# indexcol = 4, keyVal = 2 for both baseRIDs=2 and 3
# need to avoid overwriting dictionary entry with latest insert
# dict[col=4] = {2 : 2} -> overwritten with {2 : 3}
# maybe make list of possible baseIDs
# so, dict[col=4] = {2: [2,3]} 

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        #self.indices = [dict()] * table.num_columns # BUG! WRONG
        self.primary_index = table.key_index
        self.indices = [dict() for _ in range(table.num_columns)] # corrected        

    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        if key_val not in self.indices[column].keys():
            raise KeyError
        # Return baseID(s) associated with key_val based on column_number
        # Dictionaries for primary keys have a 1:1 mapping with their baseIDs
        return self.indices[column][key_val] # Returns a list of baseIDs if non-primary key else single baseID


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
        # Remove index on specific column
        self.indices = self.indices[column_number + 1:]
