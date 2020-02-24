from template.config import *
from collections import defaultdict
#from BTrees.OOBTree import OOBTree

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""

#Dealing with non unique key values for the same indexed column
#Example
# indexcol = 4, keyVal = 2 for both baseRIDs=2 and 3
# need to avoid overwriting dictionary entry with latest insert
# dict[col=4] = {2 : 2} -> overwritten with {2 : 3}
# maybe make list of possible baseIDs
# so, dict[col=4] = {2: [2,3]} 

class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.primary_index = table.key_index
        self.num_columns = table.num_columns
        self.table = table
        self.indices = []

        #self.update_map_copy = [0 for _ in range(table.num_columns)] #NOTE added this
        # Each entry is contains the number of updates per column
        # If update_map_copy[col] != Table's update_map[col] && selection needed -> need to rebuild indexer on col

        # Only have a dictionary of key:val for the primary key column
        # For the rest of the columns create a defauldict(list) type
        # This just creates a list as value for each key -- key: [val1, val2, ...] when the value is appended to the list
        for col in range(table.num_columns):
            self.indices.append(defaultdict(list))
    
        self.indices[self.primary_index] = dict()

             

    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        try:
            matches = self.indices[column][key_val] # List of baseIDs if non-primary key else single baseID
        except:
            matches = INVALID_RECORD # Indicate no entry for key_val, column pair
        return matches


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column): 
        baseIDs = []
        for cur_key in range(begin, end + 1):
            result = self.locate(cur_key, column)
            if isinstance(result, int):
                if result == INVALID_RECORD:
                    continue # No match found
                result = [result] # Single match found, convert to list
            # Add result to list of baseIDs
            baseIDs += result
        return baseIDs # Return a single list

    """
    # Updates dictionary for key replacement
    """
    def update_primaryKey(self, key, new_key, column_number):
        # Search and replace dictionary entry
        base_rid = self.locate(key, self.primary_index)
        self.indices[column_number][new_key] = base_rid
        # Don't pop because of select in main
        #self.indices[self.primary_index].pop(key)
    
    def insert_primaryKey(self, key, val):
        # Insert key-value pair into primary key
        self.indices[self.primary_index].update({key:val})


    """
    # Build a Index on given column_number
    """

# select col3 -> build for first time, len > 0 now
# update col3
# select col3 -> already built, does nothing // need to update!

    def create_index(self, column_number):        
        # Collect latest RIDs for each baseID
        baseIDs = list(self.indices[self.primary_index].values()) 
        
        #if self.update_map_copy[column_number] == self.table.update_map[column_number]:
        #    return # Avoiding rebuilding whole column everytime if select (entries already up to date)
        # I realized that this rarely happens for given testers. It's always updating columns for X times...

        # Then get their latest keys for column_number via read_records()
        for rid in baseIDs:
            [page_range_index, page_row, byte_pos] = self.table.page_directory[rid]
            page_range = self.table.page_range_collection[page_range_index]
            # Check if rid is baseID or tailID
            cur_pages = page_range.base_set[page_row]
            # Read its data
            base_data = cur_pages[INIT_COLS + self.primary_index].data
            base_key = int.from_bytes(base_data[byte_pos:byte_pos + DATA_SIZE], 'little')

            query_cols_index = []
            for _ in range(self.num_columns):
                query_cols_index.append(0)
            query_cols_index[column_number] = 1


            record = self.table.read_records(base_key, self.primary_index, query_cols_index)[0]
            latest_key = record.columns[column_number]
            
            self.indices[column_number][latest_key].append(rid)
            
        # Since we've rebuilt entire index, update 'update_map_copy' 
        #self.update_map_copy[column_number] = self.table.update_map[column_number]       
    

    """
    Drop index of specific column
    """
    def drop_index(self, table, column_number):
        # Remove index on specific column
        # Replace with new dictionary
        self.indices[column_number] = defaultdict(list) # Empty placeholder
