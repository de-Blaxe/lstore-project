# Global Settings for the Database

## Byte-based
PAGE_SIZE = 4096
DATA_SIZE = 8

## Decimal-based
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_RID_COLUMN = 4
TPS_COLUMN = 5
INIT_COLS = 6

BUFFER_SIZE = 10 # Max number of Page Sets in BufferPool
PAGE_CAPACITY = 512 # Max Records per Page
PAGE_RANGE_FACTOR = 4 # Max Base Sets per Page Range

## FLAGS
INVALID_RECORD = 0
EXCLUSIVE_LOCK = -1
NO_READERS = 0
