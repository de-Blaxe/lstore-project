from template.config import *

# Make sure to install the module: pip3 install BTrees
from BTrees.OOBTree import OOBTree

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

class Index:

    def __init__(self, table=None): # Added parameter "table"
        # Create a BTree for each of the index columns
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns # [None, None, None, None, None]
        
        for i in range(len(self.indices)):
            # Create BTree for every column to be indexed
            self.indices[i] = OOBTree()
        

    """
    # Template Code
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column): # Added "column" parameter
        if key_val not in self.indices[column].keys():
            raise KeyError
        # return value associated with key_val based on column_number
        return self.indices[column][key_val]


    """
    # Template Code
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column): # Added function definition
        pass

    """
    # Get indexed positions of records with matching key value
    """
#     def get_positions(self, key_val, max_key_val = None):
#         if self.last_index_length != len(self.index):
#             self.index = sorted(self.dictionary.items())
#             self.last_index_length = len(self.index)
#         if max_key_val == None:
#             max_key_val = key_val
# 
#         output = []
#         # Copy all key_vals into a list
#         tuple_first = lambda x: x[0]
#         key_index = list(map(tuple_first, self.index))
# 
#         # Search for given key value in list of keys
#         try:
#             found_index = bisect.bisect_left(key_index, key_val)
#         except ValueError:
#             return []
#         else:
#             try:
#                 if key_val != self.index[found_index][0]:
#                     return []
#             except:
#                 pass
# 
#             output.append(found_index)
# 
#             i = found_index + 1
#             while i < len(key_index) and key_val <= self.index[i][0] <= max_key_val:
#                 output.append(i)
#                 i += 1
#             return output


    """
    # Inserts new entry (key, val) into dictionary
    """
#     def insert(self, key, val, column_number):
#         #self.index.append((key, val)) # NOTE: Kept this code, otherwise "IndexError: list index out of range" 
#         # Insert {key: val} in BTree -- Mapping key to base RID
#         dictionary = dict()
#         dictionary[key] = val
#         self.indices[column_number].update(dictionary)

            
    """
    # Updates dictionary for key replacement
    """
    def update_index(self, key, new_key, column_number):
        # List of matching keys must be of length 1 if the key is unique
        base_rid = self.locate(key, column_number)
        dictionary = dict()
        dictionary[new_key] = base_rid
        self.indices[column_number].update(dictionary)
        # Remove previous mapping?
        self.indices[column_number].remove(key)
        #self.last_index_length = -1
        # Force re-sort

    """
    Create index on specific column
    """
    def create_index(self, key, val, column_number):
        # {col value : RID}
        # Insert {key: val} in BTree -- Mapping key to base RID
        dictionary = dict()
        dictionary[key] = val
        self.indices[column_number].update(dictionary)
        
    """
    Drop index of specific column
    """
    def drop_index(self, table, column_number):
        # Remove index on specific column
        for key in self.indices[column_number].keys():
            remove(key)
