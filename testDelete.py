from template.db import Database
from template.query import Query
from template.config import *

from time import process_time
from random import randint, sample

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}


# Measure Insertion Time
insert_time_0 = process_time()
for i in range(0, 5): # Inserts 5 Records
    key = 9000 + i # Generate unique keys
    records[key] = [key, randint(0,20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    print('inserted', records[key])
insert_time_1 = process_time()
print("Inserting 5 records took:  \t\t\t", insert_time_1 - insert_time_0)

# Measure Selection Time
select_time_0 = process_time()
for key in records:
    record = query.select(key, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key , ':', record, ', correct:', records[key])
    #else:
    #    print('select on', key, ':', record)
select_time_1 = process_time()
print("Selecting records took:  \t\t\t", select_time_1 - select_time_0)


# Measure Update Time
update_time_0 = process_time()
for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        query.update(key, *updated_columns)
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        print("Updated column i=", i, " with value: ", updated_columns[i])
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
        elif i == grades_table.num_columns - 1: # Print latest Record once
            print('Latest record: ', record.columns, "\n") 
        updated_columns[i] = None
update_time_1 = process_time()
print("Updating records took:  \t\t\t", update_time_1 - update_time_0) 

keys = sorted(list(records.keys()))
expected = []

for c in range(0, grades_table.num_columns):
    expected.append(sum(records[key][c] for key in range(9000,9005)))
    print("Expected sum for column=", c, "is ", expected[c])

for c in range(0, grades_table.num_columns):
    result = query.sum(9000, 9005, c)
    if expected[c] != result:
        print("Sum error. Expected: ", expected[c], "but Got: ", result)

# TODO: Check deletion
# Delete one record: Say, the first Record (with RID=9000)
query.delete(9000)
first_sum = [0,0,0,0,0]
for c in range(0, grades_table.num_columns):
    first_sum[c] = records[9000][c]

print("\nInitial Sum: ", expected)
print("Sum to ignore: ", first_sum)

for c in range(0, grades_table.num_columns):
    result = query.sum(9000, 9005, c)
    if expected[c] - first_sum[c] != result:
        print("Sum Error after Deletion")
    else:
        print("Success! Deleted Record was skipped over.")
        print("Expected Reduced sum: ", expected[c] - first_sum[c], " and Your Result: ", result, "\n")
