from template.config import *
from collections import defaultdict

"""
A data structure holding indices for various columns of a table. 
Key column should be indexd by default, other columns can be indexed through this object. 
Indices are usually B-Trees, but other data structures can be used as well.
"""


class Index:

    def __init__(self, table):
        # One index for each table. All are empty initially.
        self.primary_index = table.key_index
        self.num_columns = table.num_columns
        self.table = table
        self.indices = []

        # Only have a dictionary of key:val for the primary key column
        # For the rest of the columns create a defauldict(list) type
            # This just creates a list as value for each key
            # key: [val1, val2, ...] where the value is appended to the list

        for col in range(table.num_columns):
            self.indices.append(defaultdict(list))
        
        self.indices[self.primary_index] = dict()
     

    """
    # Returns the rid of all records with the given key value on column "column"
    """
    def locate(self, key_val, column):
        try:
            # Return list of baseIDs if non-primary key; else, single baseID
            matches = self.indices[column][key_val]
        except:
            # Indicate no entry for key_val, column pair
            matches = INVALID_RECORD # Defined as 0 in config
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
    def create_index(self, column_number):
                # Collect latest RIDs for each baseID
        baseIDs = list(self.indices[self.primary_index].values()) 
        
        # Get their latest keys for column_number via read_records()
        for rid in baseIDs:
            [page_range_index, page_row, byte_pos] = self.table.page_directory[rid]
            page_range = self.table.page_range_collection[page_range_index]

            # Check if rid is baseID or tailID
            cur_pages = self.table.memory_manager.get_pages(page_range.base_set[page_row], self.table)

            # Read its data
            base_data = cur_pages[INIT_COLS + self.primary_index].data
            base_key = int.from_bytes(base_data[byte_pos:byte_pos + DATA_SIZE], 'little')

            # Init values
            query_cols_index = []
            for _ in range(self.num_columns):
                query_cols_index.append(0)
            query_cols_index[column_number] = 1 
            
            # Check for old baseIDs and remove from key
            # Check if baseID present in other mappings by looking through keys
            # If found -- return the key
            match_key = None
            key_list = list(self.indices[column_number].keys())
            rid_list = list(self.indices[column_number].values())
            
            if len(key_list) > 0 and len(rid_list) > 0:
                try:
                    key_list[rid_list.index(rid)]
                    self.indices[match_key].remove(rid)
                except:
                    pass
                     

            record = self.table.read_records(base_key, self.primary_index, query_cols_index)[0]
            latest_key = record.columns[column_number]
            self.indices[column_number][latest_key].append(rid)
            

    """
    Drop index of specific column
    """
    def drop_index(self, table, column_number):
        self.indices[column_number] = defaultdict(list) # Empty placeholder
