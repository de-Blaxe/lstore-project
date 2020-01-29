from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(page_size)
        self.first_unused_byte = 0

    def has_capacity(self, needed_space = 0):
        return self.first_unused_byte + needed_space <= page_size
        pass

    def write(self, value):
        self.num_records += 1
        # assuming the value is always 64 bits
        self.data = self.data[:self.first_unused_byte] + value.to_bytes(8, 'little')
        pass

