# Global Setting for the Database
# PageSize, StartRID, etc..

page_size = 4096

# declaring a constant so we know what to change once data size becomes variable
data_size = 8
# new_RID = self.num_records + (page_size * num_pages)
# or
# row_location: str

# new_RID = str(page_size*num_pages) + str(row_location)
