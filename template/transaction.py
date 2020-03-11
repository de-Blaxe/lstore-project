from template.table import Table, Record
from template.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.expected_results = []
        pass


    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args): 
        self.queries.append((query, args))

    def add_expected_results(self, desired_columns):
        self.expected_results.append(desired_columns)

    # If you choose to implement this differently,
    # this method must still return True if transaction commits 
    # or False on abort
    def run(self):
        for count, request in enumerate(self.queries):
            [query, args] = request
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
            # This condition is tailored to txn_reads_only.py (selecting by primary key --> list of one record returned)
            if result[0].columns != self.expected_results[count]:
                print("Select error. Query returned: ", result[0].columns, " but Expected: ", self.expected_results[count], "\n")
                #return
            else:
                print('Pass\n')
        return self.commit() # Commit iff all queries within Transaction succeed


    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False


    def commit(self):
        # TODO: LATER commit to database
        return True

"""
Txn Worker -> Thread
txns = [Txn1, Txn2, Txn3, ...] Txn 2 fails -> move onto next Txn (Txn3 onwards)???

# Actually don't think so bc of template code (run() returns early if result == False)
"""
