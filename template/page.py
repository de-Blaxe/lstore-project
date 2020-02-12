from template.config import *

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(PAGE_SIZE)
        self.first_unused_byte = 0

    """
    # This Page method is never called 

    def has_capacity(self):
        return self.first_unused_byte <= PAGE_SIZE
    """

    def write(self, value, position=None):
        # Determine if indirection byte replacement needed
        if position == None:
            position = self.first_unused_byte
            self.num_records += 1
            self.first_unused_byte += DATA_SIZE
        # Perform write
        self.data[position:DATA_SIZE + self.first_unused_byte] = value.to_bytes(DATA_SIZE, 'little') if type(value) == int else value 
