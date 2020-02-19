from template.db import Database # changed from lstore.db
from template.query import Query # changed from lstore.query

from time import process_time

from random import choice, randint, sample, seed

db = Database()
db.open('~/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}
seed(3562901)

insert_time0 = process_time()
for i in range(0, 3): # Inserting & Selecting three records
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")
insert_time1 = process_time()
print("Insert took ", insert_time1-insert_time0)


select_time0 = process_time()
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
         print('select on', key, ':', "Record RID=", record.rid, " columns", record.columns)
print("Select finished")
select_time1 = process_time()
print("Select took: ", select_time1-select_time0)

print("\n After selection: Check Status of Table Indexer.")
prompts = ["SIDs", "Grade1", "Grade2", "Grade3", "Grade4"]
for i in range(0,5):
    print("Dictionary[", i, "] maps ", prompts[i], " to baseIDs and contains: ", grades_table.indexer.indices[i], "\n")

"""
for _ in range(0,1):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns): 
            # This loop does not update primary key value (SIDs)
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


"""
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
"""
