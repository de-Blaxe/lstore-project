from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(page_size)
        self.first_unused_byte = 0

    def has_capacity(self):
        return self.first_unused_byte <= page_size

    def write(self, value, position=None):
        self.num_records += 1
        
        # Determine if indirection byte replacement needed
        if position == None:
            position = self.first_unused_byte
        
        if self.has_capacity() or position is not None:
            ## Make alias
            ##data_slots = self.data[position:data_size + self.first_unused_byte]
            try:
                self.data[position:data_size + self.first_unused_byte] = value.to_bytes(data_size, 'little') if type(value) == int else value
            except:
                print("EXCEPTION: ", value, "\n")
            if position == self.first_unused_byte:
                self.first_unused_byte += data_size
