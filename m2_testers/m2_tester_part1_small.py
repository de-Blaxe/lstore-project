from template.db import Database
from template.query import Query

from random import choice, randint, sample, seed

from time import process_time

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

insert_time_0 = process_time()
records = {}
seed(3562901)
for i in range(0, 1000): # changed from 1000 (1k)
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")
insert_time_1 = process_time()
print("Inserting 1k records took: \t\t\t", insert_time_1 - insert_time_0, "\n")

select_time_0 = process_time()
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
print("Select finished")
select_time_1 = process_time()
print("Selecting 1k records took: \t\t\t", select_time_1 - select_time_0, "\n")


update_time_0 = process_time()
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
                print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
            else:
                print('update on', original, 'and', updated_columns, ':', record.columns)
            updated_columns[i] = None
print("Update finished")
update_time_1 = process_time()
print("Updating 4k times took: \t\t\t", update_time_1 - update_time_0, "\n")



print("Update to Pg Range dictionary: ", grades_table.update_to_pg_range, "\n")
#print("Merge Flag after updates: ", grades_table.merge_flag, "\n")
print("Number Merged:", grades_table.num_merged, "\n")


sum_time_0 = process_time()
for i in range(0, 25): # Changed from 100 to 25 bc it took too long
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    # else:
    #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
print("Aggregate finished")
sum_time_1 = process_time()
print("25 summation calls took: \t\t\t", sum_time_1 - sum_time_0, "\n")


db.close()