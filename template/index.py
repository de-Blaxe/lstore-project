from template.config import *

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.primary_index = table.key_index
        self.indices = [dict() for _ in range(table.num_columns)]        

    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        """
        if key_val not in self.indices[column].keys():
            raise KeyError
        """ # account for column aggregation

        # Return baseID(s) associated with key_val based on column_number
        # Dictionaries for primary keys have a 1:1 mapping with their baseIDs
        return self.indices[column][key_val] # Returns a list of baseIDs if non-primary key else single baseID


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column): 
        baseIDs = []
        for cur_key in range(begin, end + 1):
            baseIDs += self.locate(cur_key, column) # Locate() may return a list
        return baseIDs # Return a single list


    """
    # Updates dictionary for key replacement
    """
    def update_index(self, key, new_key, mapped_rid, column_number): 
        # Search and replace dictionary entry
        """
        # Previous code only works iff unique key values:
        base_rids = self.locate(key, column_number)
        self.indices[column_number][new_key] = base_rids # buggy because this will copy over all base_rids list
        # Delete old key in dictionary
        self.indices[column_number].pop(key) # buggy because this will delete entire base_rids list
        """

        #### Example ####
        # Grade1 Dictionary has {90: [1,3,5]}
        # Want to only update Record with baseID=1 with Grade1 = 100

        # Expected output
        # Grade1 Dictionary has {90: [3,5], 100: [1]} # need to split them up!
        
        # Check if new_key is unique 
        try:
            self.indices[column_number][new_key]
        except KeyError:
            self.indices[column_number][new_key] = []
        self.indices[column_number][new_key].append(mapped_rid) 

        # Delete mapped_rid from previous mapping
        cur_vals = self.indices[column_number][key]
        if column_number == self.primary_index or len(cur_vals) == 1:
            #print("Dictionary[", column_number,"] to remove: key=", key, " : mapped_baseID=", mapped_rid, "\n")
            self.indices[column_number].pop(key)
            #self.indices[column_number][key].remove(mapped_rid)
        #elif len(cur_vals) == 1: # [single baseID]
            # Safe to remove old key completely   
        #    self.indices[column_number].pop(key)
        else:
            # There are other baseIDs mapped to key
            cur_vals.remove(mapped_rid)

        

    """
    Create index on specific column
    """
    def create_index(self, key, val, column_number):
        # Dictionary Entry: {col value : [baseID]}
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
        self.indices[column_number] = {}
