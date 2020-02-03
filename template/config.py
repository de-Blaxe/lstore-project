# Global Settings for the Database

## Byte-based

PAGE_SIZE = 4096
DATA_SIZE = 8

## Decimal-based

INVALID_RECORD = -1

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
INIT_COLS = 4

PAGE_CAPACITY = 512


## Previous Ideas:
# new_RID = self.num_records + (page_size * num_pages)
# row_location: str
# new_RID = str(page_size*num_pages) + str(row_location)
