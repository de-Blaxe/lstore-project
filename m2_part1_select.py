from template.db import Database # changed from lstore.db
from template.query import Query # changed from lstore.query

from time import process_time

from random import choice, randint, sample, seed

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0) # later change back to 0
query = Query(grades_table)

records = {}
seed(3562901)

insert_time0 = process_time()
for i in range(0, 10): # changed from 1k -> happens 10 times for now... 
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)] 
    query.insert(*records[key])
    print("inserted: ", records[key])
keys = sorted(list(records.keys()))
print("Insert finished")
insert_time1 = process_time()
print("Insert took ", insert_time1-insert_time0)


"""
# NOTE: SELECT TESTER MODIFIED
# CHECKS IF ONE OF RECORD OBJ RETURNED IS CORRECT
# No longer compares expected Record against 0th Record in List of Records
"""
select_time0 = process_time()

select_index = 1
for key in keys:
    key_val = records[key][select_index]
    list_records = query.select(key_val, select_index, [1, 1, 1, 1, 1]) 
    error = False
    """
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    """
    # Checks if one of Record obj returned is correct
    for record in list_records:
        if record.columns == records[key]:
            error = False
            break
    # Checks if error flag has been updated
    if error:
        print('select error on', key, 'correct:', records[key])
    else:
        # Since we break out of loop once correct Record found
        print('select on', key, ':', records[key])

print("Select finished")
select_time1 = process_time()
print("Select took: ", select_time1-select_time0)

update_time0 = process_time()
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
            else:
                print('update on', original, 'and', updated_columns, ':', record.columns)
            updated_columns[i] = None
print("Update finished")
update_time1 = process_time()
print("Update took: ", update_time1-update_time0)


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
sum_time1 = process_time()
print("Sum took: ", sum_time1-sum_time0)
db.close()
