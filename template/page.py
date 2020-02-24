from template.config import *

class Page:

    def __init__(self, num_records = 0, first_unused_byte = 0, data = bytearray(PAGE_SIZE)):
        self.num_records = num_records
        self.first_unused_byte = first_unused_byte
        self.data = data


    """
    # Checks if Page has space
    """ 
    def has_space(self):
        return self.first_unused_byte < PAGE_SIZE
   
    
    """
    # Modifies record data within Page
    """
    def write(self, value, position=None):
        # Determine if indirection byte replacement needed
        if position == None:
            position = self.first_unused_byte
            self.num_records += 1
            self.first_unused_byte += DATA_SIZE
        # Perform write
        # Modified this logic
        self.data[position:DATA_SIZE + position] = value.to_bytes(DATA_SIZE, 'little') if type(value) == int else value 
        # My bad -- this lead to the bug
        #self.first_unused_byte += DATA_SIZE
