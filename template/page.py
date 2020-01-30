from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(page_size)
        self.first_unused_byte = 0

    def has_capacity(self):
        return self.first_unused_byte <= page_size

    def write(self, value):
        self.num_records += 1

        if self.has_capacity():
            # assuming the value is always 64 bits and all values are non-negative
            self.data[self.first_unused_byte: data_size + self.first_unused_byte] = value.to_bytes(data_size, 'little')
            self.first_unused_byte += data_size 

