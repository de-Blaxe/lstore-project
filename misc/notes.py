# NOTES: IN PROGRESS PAGE RANGE IMPLEMENTATION & MODIFIED INSERTIONS
PAGE RANGE CLASS:
base_set = []
tail_set = []
max_baseRows = 4 # aka config::PAGE_RANGE_FACTOR
# BUT there's no maxTailRows, can be appended without limit
# granularity of page range based on tail pages

# initializing base_set beforehand - since the size is fixed
# total_pages = INIT_COLS + table.num_columns
base_pages = [Page() for _ in range(total_pages)]
for _ in range(max_baseRows):
	self.base_set.append(base_pages)

# There should be (max_baseRows * total_pages) BasePage()'s within a Page Range object
"""
base_set = [[Row0: Page1, ..., Pagek], 
	    [Row1: Page1, ..., Pagek], 
            [Row2: Page1, ..., Pagek], 
            [Row3: Page1, ..., Pagek]]

# if we define max_baseRows = 4
# and if we have total_pages = k (meta + user data columns)

"""

# Replaced last_LID/TID_used with these instead:
last_base_row = 0 # initially there's a single baseRow
last_tail_row = 0 

=====================
TABLE CLASS:

# let RID be either a baseID or tailID (shouldn't matter)
PAGE_DIRECTORY[RID] = [page_range_index, page_row] 
# Given RID, Directory returns a list ("the coordinates of a Record within a Page Range")

# Inserting Base Records Flow
QueryInsert() --> TableInsertBaseRecord() --> TableWriteToBasePages()

TableInsertBaseRecord():
	1. Error check: make sure unique RID (baseID)
	2. See if Page Range object exists for given RID
		- Find page_range_index (while loop)
		- Index page_range_collection with that index above
		- If Page Range DNE, allocate/append new Page_Range to collection
	3. Either case...
		- Get matching Page Range for RID
		- Access its base_set
		- Access most recently used baseRow within base_set
		- Determine if basePage within baseRow is FULL
			 + if full: 
                * increment last_base_row
                * allocate new set of Base Pages, one per col
			 + else: Write to the current baseRow
	4. Write meta data
	5. Write user input data
	6. Update Page Directory for given RID
		- page_directory[rid] = [page_range_index, page_row]
	7. Create new entry for Indexer --> Indexer.insert()

"""
TableInsertTailRecord():
    1.
"""
