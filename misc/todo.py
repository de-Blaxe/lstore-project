* Checklist: Phase 2 *

[x] inserting base records + checking its correctness
    - worked with very basic select() --> tableSelect.py

[todo] inserting tail records 
    - need to check correctness by implementing select()

[x] updated BaseRID columns for tail records

[todo] selecting any record

[todo] need to index on all columns of record

[todo] create bufferpool, disk (file)
    - write to, read from...

[todo] eviction policy for bufferpool
    - this depends on Pinning Pages

[todo] background merging + updating TPS column for base records


# Should we work on selecting records or indexing first?
# if we do select first, we can assume primary key for now
# and check correctness of updating records?
