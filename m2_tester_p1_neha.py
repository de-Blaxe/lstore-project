from lstore.db import Database
from lstore.query import Query

from random import choice, randint, sample, seed

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}
seed(3562901)

insert_time0 = process_time()
for i in range(0, 10):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    # else:
    #     print('select on', key, ':', record)
    #else:
     #    print('select on', key, ':', record)
"""
# Testing for select on column other than primary key
# Doesn't make sense to loop over all the keys since we're not selecting based on this key anymore
# Need to modify this to test on other columns because now we can return multiple records (more than one row)
select_index = 1
key_val = 92106429 
records_list = []
records_list = query.select(key_val, select_index, [1, 1, 1, 1, 1])
error = False
# Now we check for correctness of each record
key_counter = 0
for record in records_list:
    for i, column in enumerate(record.columns):
        #print("records = ", records)
        if column != records[keys[key_counter]][i]:
            error = True
            
    key_counter += 1
    
    if error:
        for col in record.columns:
            print("my columns = ", col)
        print('select error on', key, ':', record, ', correct:', records[key])
    
print("Select finished")

for _ in range(10):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            original = records[key].copy()
            records[key][i] = value
            query.update(key, *updated_columns)
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
            error = False
            for j, column in enumerate(record.columns):
                if column != records[key][j]:
                    error = True
            if error:
                print('update error on', original, 'and', updated_columns, ':', record, ', correct:', records[key])
            # else:
            #     print('update on', original, 'and', updated_columns, ':', record)
            updated_columns[i] = None
print("Update finished")


"""
sum_time0 = process_time()
for i in range(0, 100):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    # else:
    #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
print("Aggregate finished")
db.close()
