from template.table import Table, Record
from template.index import Index
from template.transaction import Transaction

import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self):
        self.transactions = []
        #self.txn_thread = threading.Thread(target=self.run, args=[]) [but don't know when to call start()]  
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # txn_worker = TransactionWorker([t])
    # th1 = threading.Thread(target=txn_worker.run)
    """

    def add_transaction(self, transaction):
        # Append new set of queries
        self.transactions.append(transaction)
        pass

    def run(self):
        for txn in self.transactions:
            txn_thread = threading.Thread(target=txn.run, args=[])
            txn_thread.start()
            #txn.run() [template code]
            # Maybe in Table Class, if we need to abort (resource not ready), we raise an Exception
            # Exception caught by Transaction.run() method -> knows if it can abort or commit
        pass
