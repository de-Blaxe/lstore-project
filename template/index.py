from template.config import *
from collections import defaultdict
#from BTrees.OOBTree import OOBTree

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
        self.table = table
        #self.indices = [dict() for _ in range(table.num_columns)] # corrected  
        #self.indices = [OOBTree() for _ in range(table.num_columns)]
        self.indices = []
        self.check_prev_update = dict()
        self.indices.insert(self.primary_index, dict())
        for _ in range(1, table.num_columns):
            self.indices.append(defaultdict(list))
             

    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        
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
    def update_primaryKey(self, key, new_key, column_number):
        # Search and replace dictionary entry
        base_rid = self.locate(key, self.primary_index)
        self.indices[self.primary_index][new_key] = base_rid
        # Don't pop because of select in main
        #self.indices[self.primary_index].pop(key)
    
    def insert_primaryKey(self, key, val):
        # Insert key-value pair into primary key BTree
        self.indices[self.primary_index].update({key: val})

    
    #Create index on specific column
    def create_index(self, column_number):
        
        self.table.build_columnIndex(column_number, self.indices[column_number])
        
   
    """
    Drop index of specific column
    """
    def drop_index(self, table, column_number):
        # Remove index on specific column
        self.indices = self.indices[column_number + 1:]
