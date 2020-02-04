from template.config import *
import bisect
"""
# optional: Indexes the specified column of the specified table to speed up select queries
# This data structure is usually a B-Tree
"""


class Index:

    def __init__(self):
        '''
        index = [(int.from_bytes(self.page_directory[baseID][INIT_COLS + self.key_index].data[baseID % PAGE_CAPACITY * DATA_SIZE:baseID % PAGE_CAPACITY * DATA_SIZE + DATA_SIZE], "little"), baseID) for baseID in range(table.LID_counter)]
        tuple_first = lambda x: x[0]
        self.index = sorted(index, tuple_first)
        '''

        '''
        for baseID in range(table.LID_counter):
            byte_pos = baseID % PAGE_CAPACITY * DATA_SIZE
            pages = self.page_directory[baseID]
            page_data = pages[INIT_COLS + self.key_index].data
            base_key = int.from_bytes(self.page_directory[baseID][INIT_COLS + self.key_index].data[baseID % PAGE_CAPACITY * DATA_SIZE:baseID % PAGE_CAPACITY * DATA_SIZE + DATA_SIZE], "little")
        '''
        self.index = [] # A list containing pairs of (key_val, val)

    """
    # returns the rid of all records with the given key value
    """

    def locate(self, key_val):
        return [self.index[i][1] for i in self.get_positions(key_val)]

    def get_positions(self, key_val):
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
            if key_val != self.index[found_index][0]:
                return []
            output.append(found_index)
            i = found_index - 1
            while i >= 0 and self.index[i][0] == key_val:
                output.append(i)
                i -= 1
            i = found_index + 1
            while i < len(key_index) and self.index[i][0] == key_val:
                output.append(i)
                i += 1
            return output

    def insert(self, key, val):
        tuple_first = lambda x: x[0]
        key_index = list(map(tuple_first, self.index))
        try:
            found_index = bisect.bisect(key_index, key)
        except ValueError:
            print("Insertion error")
        else:
            self.index.insert(found_index, (key, val)) # NOTE this is calling built-in LIST.insert()
            return

    '''
    This doesn't make any sense
    
    def update(self, key, val, new_key):
        candidates = self.get_positions(key)
        record_position = filter(lambda x: self.index[x][1] == val, candidates)[0]
        self.index[record_position] = (new_key, val)
    '''

    def unique_update(self, key, new_key):
        # List of matching keys must be of length 1 if the key is unique
        record_position = self.get_positions(key)[0]
        base_rid = self.index[record_position][1]
        # Replace old mapping
        self.index = self.index[:record_position] + self.index[record_position + 1:]
        self.insert(new_key, base_rid)

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
