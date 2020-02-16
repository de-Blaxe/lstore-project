from template.db import Database
from template.query import Query
from template.config import *

from time import process_time
from random import choice, randint, sample, seed

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

seed(3562901)

# Measure Insertion Time
insert_time_0 = process_time()
for i in range(0, 10): # change back to 1k
    key = 92106429 + randint(0, 9000)
    while key in records:
        key = 92106429 + randint(0, 9000)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    #print('inserted', records[key]) # Reduce latency of insert performance
insert_time_1 = process_time()
print("Inserting 10///1k records took:  \t\t\t", insert_time_1 - insert_time_0)

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
print("Selecting 10//1k records took:  \t\t\t", select_time_1 - select_time_0)

"""
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
        error = False
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
        #else: # Reduce latency of update performance
        #    print('update on', original, 'and', updated_columns, ':', record) 
        updated_columns[i] = None
update_time_1 = process_time()
print("Updating 40///4k records took:  \t\t\t", update_time_1 - update_time_0) # 4k total updated, based on output.txt
"""


"""
# Measure Sum/Column Aggregation Time
sum_time_0 = process_time()
keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns):
    for i in range(0, 20):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        #else: # Reduce latency of sum performance
        #    print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
sum_time_1 = process_time()
print("Aggregating 100 times took:  #TODO: Edit back tabs
#ttt", sum_time_1 - sum_time_0) # 100 calls to querySum, based on output.txt"""
