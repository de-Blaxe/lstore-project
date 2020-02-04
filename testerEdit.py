from template.db import Database
from template.query import Query
from template.config import *

from random import choice, randint, sample, seed
# from colorama import Fore, Back, Style

# Student Id and 4 grades
# init()
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

seed(3562901)

# INSERTION TEST
for i in range(0, 10):
    key = 92106429 + randint(0, 9000)
    while key in records:
        key = 92106429 + randint(0, 9000)
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    print('inserted', records[key], "\n")

# SELECTION TEST
for key in records:
    record = query.select(key, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('WRONG - select error on', key , ':', record, ', correct:', records[key])
    else:
        print('CORRECT - select on', key, ':', record)

# UPDATE TEST
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
        else:
            print('update on', original, 'and', updated_columns, ':', record) 
        updated_columns[i] = None


# SUM TEST
keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns): # Repeat for each column in GradesTable (5x)
    for i in range(0, 5): # Repeat inner loop 5x
        r = sorted(sample(range(0, len(keys)), 2)) # Sample two numbers between 0 and total amt of keys
        # r: list of possible indices , to index the 'keys' list
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
        else:
            print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)

