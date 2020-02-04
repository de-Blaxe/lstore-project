from template.db import Database
from template.query import Query
from template.config import * # TODO: replace back with: init

from random import choice, randint, sample
# from colorama import Fore, Back, Style #TODO: Put back in

# Student Id and 4 grades
#init()
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

rand_keys = []
for i in range(0, 5):
    key = 92106429 + randint(0, 9000)

    # To avoid duplicate keys, generate another random key
    while key in records:
        key = 92106429 + randint(0, 9000)
    
    # Most recent key
    rand_keys.append(key)
    records[key] = [key, 20, 30, 40, 50]
    query.insert(*records[key])

for key in records.keys():
    print("keys = ", key, "record data = ", records[key])

"""
for key in records:
    record = query.select(key, [1, 1, 1, 1, 1])[0]
    print("record key in select (ours) = ", record.key)
    print("length = ", len(record.columns))
    for column in record.columns:
        print("columns in record = ", column)

    for i, column in enumerate(record.columns):
        print(i)
        print("column in record = ", column)
        if column != records[key][i]:
            #print(Fore.RED + 'Select error on key', key)
            print("SELECT ERROR on KEY ", key)
            exit()
#print(Fore.GREEN + 'Passed SELECT test.')
print('Passed SELECT Test!!!\n')
"""

for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns): # Repeat for 4x per record (i=1,2,3,4)
        value = randint(0, 20)
        updated_columns[i] = value
        records[key][i] = value
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        query.update(key, *updated_columns)
        record = query.select(key, [1, 1, 1, 1, 1])[0]
        for j, column in enumerate(record.columns):
            if column != records[key][j]:
                #print(Fore.RED + 'Update error for key', key)
                #print(Fore.RED + 'Should have been', updated_columns)
                #print(Fore.RED + 'Returned result:', record)
                print('Update error for key', key)
                print('Should have been', updated_columns)
                print('Returned result: ', record.columns)
                exit()
        updated_columns[i] = None

#for key in records:
#        for val in record.columns:
#            print("AFTER UPDATING, RECORD KEY: ", key, " COL VAL: ", val, "\n") 
#print(Fore.GREEN + 'Passed UPDATE test.')
#print("Passed UPDATE Test!!!\n")

"""
keys = sorted(list(records.keys()))
for c in range(0, grades_table.num_columns):
    for i in range(0, 20):
        r = sorted(sample(range(0, len(keys)), 2))
        column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
        result = query.sum(keys[r[0]], keys[r[1]], c)
        if column_sum != result:
            print(Fore.RED + 'Sum error for keys', keys[r[0]], 'to', keys[r[1]], 'on column', c)
            print(Fore.RED + 'Should have been', column_sum, ' but returned ', result)

#print(Fore.GREEN + 'Passed SUM test.')
print("Passed SUM Test!!!\n")
"""
