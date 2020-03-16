from template.table import Table, Record
from template.index import Index

import threading # To print out current ThreadID, remove later

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass


    def add_transaction(self, t):
        self.transactions.append(t)


    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # transaction_worker = TransactionWorker([t])
    """
    def run(self):
        #print("Transaction Worker now running. Its ThreadID is: ", threading.get_ident())
        for transaction in self.transactions:
            # Each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # Stores the number of transactions that committed
        print("STATS: ", self.stats)
        table = self.transactions[-1].queries[-1][0].__self__.table
        self.result = len(list(filter(lambda x: x, self.stats)))
