from template.config import *
import bisect

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

#NOTE: Merged Milestone 2 Template Code for index.py

class Index:

    def __init__(self):
        self.index = [] # A list containing pairs of (key_val, val)
        self.dictionary = {}
        self.last_index_length = len(self.index)
        """
        # Template Code

        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        pass
        """

    """
    # Template Code
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column): # Added "column" parameter
        if key_val not in self.dictionary:
            raise KeyError
        return self.dictionary[key_val]


    """
    # Template Code
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column): # Added function defintion
        pass

    def get_positions(self, key_val, max_key_val = None):
        if self.last_index_length != len(self.index):
            self.index = sorted(self.dictionary.items())
            self.last_index_length = len(self.index)
        if max_key_val == None:
            max_key_val = key_val

        output = []
        # Copy all key_vals into a list
        tuple_first = lambda x: x[0]
        key_index = list(map(tuple_first, self.index))

        # Search for given key value in list of keys
        try:
            found_index = bisect.bisect_left(key_index, key_val)
        except ValueError:
            return []
        else:
            try:
                if key_val != self.index[found_index][0]:
                    return []
            except:
                pass

            output.append(found_index)

            i = found_index + 1
            while i < len(key_index) and key_val <= self.index[i][0] <= max_key_val:
                output.append(i)
                i += 1
            return output


    def insert(self, key, val):
        self.dictionary[key] = val
        self.index.append((key, val)) # NOTE: Kept this code, otherwise "IndexError: list index out of range" 

    def unique_update(self, key, new_key):
        # List of matching keys must be of length 1 if the key is unique
        base_rid = self.locate(key)
        # Force re-sort
        self.dictionary[new_key] = base_rid
        self.last_index_length = -1


    """
    # optional: Create index on specific column
    """
    def create_index(self, table, column_number):
        pass


    """
    # optional: Drop index of specific column
    """
    def drop_index(self, table, column_number):
        pass
