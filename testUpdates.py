from template.db import Database
from template.query import Query
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 2):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()

print("Inserting two records took:  \t\t\t", insert_time_1 - insert_time_0)

maxID = grades_table.last_LID_used
GradeCol = 5
for i in range(0, maxID + 1):
	print("\nAfter two insertions: ", grades_table.page_directory[i][GradeCol].data[:17])
print("93 (Initial Value for Grade 1) as binary: ", (93).to_bytes(8, 'little'), "\n")


# Measuring update Performance
update_cols = [123, 87, None, None, None]

update_time_0 = process_time()
for i in range(0, 2):
    query.update(keys[i], *update_cols)
update_time_1 = process_time()
print("Updating two records took:  \t\t\t", update_time_1 - update_time_0)

print("\nAfter two updates: ", grades_table.page_directory[2**64 - 1][GradeCol].data[:17])
# print("\nAfter two updates:", grades_table.page_directory[2**64 - 2][GradeCol].data[:17], "\n")
print("87 (Updated Value for Grade 1) as binary: ", (87).to_bytes(8, 'little'), "\n")
