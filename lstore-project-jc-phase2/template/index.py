from template.config import *

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

#### Example ###
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
    def update_index(self, key, new_key, column_number): # IDEA: Add parameter: mapped_rid
        # Search and replace dictionary entry
        base_rids = self.locate(key, column_number)
        self.indices[column_number][new_key] = base_rids # buggy because this will copy over all base_rids list
        # Delete old key in dictionary
        self.indices[column_number].pop(key) # buggy because this will delete entire base_rids list
        
        #### Example ####
        # Grade1 Dictionary has {90: [1,3,5]}
        # Want to only update Record with baseID=1 with Grade1 = 100

        # Expected output
        # Grade1 Dictionary has {90: [3,5], 100: [1]} # need to split them up!
        
        """
        ##### IDEA ####
        ##### Assuming that we passed prev_rid (aka mapped_rid) as argument to update_index() ####
 
        # Check if new_key is unique
        try:
            self.indices[column_number][new_key]
        except KeyError:
            self.indices[column_number][new_key] = []
        self.indices[column_number][new_key].append(mapped_rid)

        # Delete cur_rid from previous mapping
        base_rids = self.locate(key, column_number) # aka retrieve: self.indices[column_number][key]
        if len(base_rids) > 1:
            # There are other baseIDs mapped to old key
            self.indices[column_number][key].remove(mapped_rid)
        else: # Safe to remove old key completely
            self.indices[column_number].pop(key)

        """

    """
    Create index on specific column
    """
    def create_index(self, key, val, column_number):
        # Dictionary Entry: {col value : [baseID]}
        ## Previous Idea##
        # Alternative: self.indices[column_number].update({key:val})
        #self.indices[column_number][key] = val
    
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
        #self.indices = self.indices[column_number + 1:]
        self.indices[column_number] = {} # So column_number does not get messed up
