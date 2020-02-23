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
        self.num_columns = table.num_columns
        self.table = table
        self.indices = []
        
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
        except: # Indicate no entry for key_val, column pair
            matches = INVALID_RECORD # Defined as -1
        print("matches for key_val =", key_val, " and col=", column, " --> ", matches, "\n")
        return matches


    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """
    def locate_range(self, begin, end, column): 
        baseIDs = []
        for cur_key in range(begin, end + 1):
            #baseIDs.append(self.locate(cur_key, column)) 
            #Locate() may return a list, a single baseID, or -1 so buggy

            # NOTE: Modified Logic below
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
        """
        # Might have old values still...
        # Base case: Check if Indexer has already indexed on column_number
        if len(self.indices[column_number]) != 0 or column_number != self.primary_index:
            return # No need to create an index
        """
        # NOTE:
        # either complex if build once and update everytime insert tail record -> slower insert_tailRecord
        # or inefficient if rebuild everytime we select -> slow select? // less penalty since already fast??

        # Collect latest RIDs for each baseID
        baseIDs = list(self.indices[self.primary_index].values())
        
        # Then get their latest keys for column_number via read_records()
        for rid in baseIDs:
            [page_range_index, page_row, byte_pos] = self.table.page_directory[rid]
            page_range = self.table.page_range_collection[page_range_index]
            # Check if rid is baseID or tailID
            cur_pages = page_range.base_set[page_row]
            # Read its data
            base_data = cur_pages[INIT_COLS + self.primary_index].data
            base_key = int.from_bytes(base_data[byte_pos:byte_pos + DATA_SIZE], 'little')

            query_cols = []
            for _ in range(self.num_columns):
                query_cols.append(0)
            query_cols[column_number] = 1
            
            #print("CREATE INDEX rid: ", rid, "query cols: ", query_cols, " and column to select is:", column_number)

            record = self.table.read_records(base_key, self.primary_index, query_cols)[0]
            latest_key = record.columns[column_number]
            self.indices[column_number][latest_key].append(rid)
            

    """
    Drop index of specific column
    """
    def drop_index(self, table, column_number):
        # Remove index on specific column
        #self.indices[column_number] = {} # Empty placeholder
        self.indices[column_number].clear() # Empty entries
        #self.indices = self.indices[column_number + 1:]
