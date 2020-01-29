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

        pass

