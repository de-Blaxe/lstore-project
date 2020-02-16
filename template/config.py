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
BASE_RID_COLUMN = 4
# TODO: TPS_COLUMN = 5
INIT_COLS = 5 # = 6

PAGE_CAPACITY = 512 # Max Records per Page
PAGE_RANGE_FACTOR = 4 # Max Base Rows per Page Range
