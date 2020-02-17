from template.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.first_unused_byte = 0

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
            #self.first_unused_byte += DATA_SIZE
        # Perform write
        self.data[position:DATA_SIZE + self.first_unused_byte] = value.to_bytes(DATA_SIZE, 'little') if type(value) == int else value 
        self.first_unused_byte += DATA_SIZE