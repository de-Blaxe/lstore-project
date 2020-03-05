from template.table import Table, Record
from template.index import Index
from template.transaction import Transaction

import threading

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = []
        self.result = 0
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

    def add_transaction(self, txn):
        # Append new set of queries
        self.transactions.append(txn)
        pass

    def run(self):
        for transaction in self.transactions:
            self.stats.append(transaction.run())
        self.result = sum(self.stats)
        """
        # Idea?
        for transaction in self.transactions:
            txn_thread = threading.Thread(target=transaction.run, args=[])
            txn_thread.start()
        """ 
        pass
