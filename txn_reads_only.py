"""
# NOTE: FOLLOWING TESTS FOR CONCURRENT READS ONLY (Shared Locks)
# Attempts to check for correctness of reads by declaring an 'expected_results' array per Transaction
"""

from template.db import Database
from template.query import Query
from template.transaction import Transaction
from template.transaction_worker import TransactionWorker

import threading
from random import choice, randint, sample, seed
from time import process_time

db = Database()
db.open('~/ECS165') 
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
num_threads = 8
seed(8739878934)

# Generate random records
for i in range(0, 30): # Changed from 100 Base Records to 10: Force duplicate readers
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(0,20), randint(0,20), randint(0,20), randint(0,20)] # Init with random col values
    q = Query(grades_table)
    q.insert(*records[key])

# create TransactionWorkers
transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker([]))

# Testing concurrent Reads
for i in range(1000): 
    transaction = Transaction()
    key = choice(keys) # choose one random key (one random record to read)
    q = Query(grades_table)
    transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
    transaction.add_expected_results(records[key])
    transaction_workers[i % num_threads].add_transaction(transaction)


threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(target = transaction_worker.run, args = ()))

start = process_time()
for i, thread in enumerate(threads):
    print('Thread', i, 'started.')
    thread.start()


for i, thread in enumerate(threads):
    thread.join()
    print('Thread', i, 'finished')
end = process_time()

print("Reading 1k random records took ", end - start)

num_committed_transactions = sum(t.result for t in transaction_workers)
print(num_committed_transactions, 'transaction committed.')


query = Query(grades_table)
s = query.sum(keys[0], keys[-1], 1)

"""
if s != num_committed_transactions * 5:
    print('Expected sum:', num_committed_transactions * 5, ', actual:', s, '. Failed.')
else:
    print('Pass.')
"""
# Modified above because we're only testing concurrent reads (no writing, no aborts are possible)

print("Sum should be zero:", sum(list(grades_table.lock_manager.shared_locks.values())))
print("Entire current locks dictionary:", grades_table.lock_manager.shared_locks)
