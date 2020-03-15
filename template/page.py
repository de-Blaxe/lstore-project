from template.config import *

from copy import deepcopy
import threading

class Page:
    def __init__(self, num_records = 0, first_unused_byte = 0, data = None):
        self.num_records = num_records
        self.first_unused_byte = first_unused_byte
        self.data = bytearray(PAGE_SIZE) if data is None else data
        self.page_latch = threading.Lock()

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
            # Page Latch to avoid potential data races
            self.page_latch.acquire()
            position = deepcopy(self.first_unused_byte)
            self.num_records += 1
            self.first_unused_byte += DATA_SIZE
            self.page_latch.release()
        # Perform write
        self.data[position:DATA_SIZE + position] = value.to_bytes(DATA_SIZE, 'little') if type(value) == int else value 
