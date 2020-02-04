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
    query.insert(906659671+i, 93, 0, 0, 0)
    keys.append(906659671 + i)
    
#query.insert(906659671, 93, 0, 0, 0)
#query.insert(906659672, 93, 0, 0, 0)
#    keys.append(906659671 + i)
insert_time_1 = process_time()
print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measuring update Performance
update_cols = [
     [randrange(0, 100), None, None, None, None],
     [None, randrange(0, 100), None, None, None],
     [None, None, randrange(0, 100), None, None],
     [None, None, None, randrange(0, 100), None],
     [None, None, None, None, randrange(0, 100)],
 ]

update_time_0 = process_time()
for i in range(0, 2):
     query.update(choice(keys), *(choice(update_cols)))

update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)
