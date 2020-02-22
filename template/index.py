from template.config import *
from collections import defaultdict
#from BTrees.OOBTree import OOBTree

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.primary_index = table.key_index
        self.table = table
        #self.indices = [dict() for _ in range(table.num_columns)] 
        #self.indices = [OOBTree() for _ in range(table.num_columns)]
        self.indices = []
        self.check_prev_update = dict()

        """
        # This will not be correct if primary_key != 0 ---> see compareIndex.txt
        # This will just make 0th entry always {}, not primary_index th entry (since we started with empty list 'indices')

        self.indices.insert(self.primary_index, dict())
        for _ in range(1, table.num_columns):
            self.indices.append(defaultdict(list))

        """
        
        for col in range(table.num_columns):
            if col != self.primary_index:
                self.indices.append(defaultdict(list))
        self.indices.insert(self.primary_index, dict())


    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        # Returns a list of baseIDs if non-primary key else single baseID
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
    def update_primaryKey(self, key, new_key):
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
        #self.indices = self.indices[column_number + 1:] 
        # NOTE: Should we reduce length of indices, or make empty placeholder/dictionary?
        self.indices[column_number] = {}
