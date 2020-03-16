from template.table import Table, Record
from template.index import Index
import threading

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
            """
            # This condition is tailored to txn_reads_only.py (selecting by primary key --> list of one record returned)
            if result[0].columns != self.expected_results[count]:
                print("Select error. Query returned: ", result[0].columns, " but Expected: ", self.expected_results[count], "\n")
                #return
            """
            #else:
            #    #print('Pass\n') # This is not an official tester!
        #print("Transaction commited!")
        return self.commit() # Commit iff all queries within Transaction succeed


    def abort(self):
        #TODO: do roll-back and any other necessary operations

        print("abort from transaction class")
        table = self.queries[0][0].__self__.table
        curr_threadID = threading.get_ident()
        latch = table.memory_manager.latches[table.name]
        table.rollback_txn(curr_threadID, latch)
        # Release all locks
        self.release_locks()
        return False


    def commit(self):
        # TODO: LATER commit to database

        print("commit from transaction class")
        # Release Locks
        self.release_locks()
        return True


    def release_locks(self):
        table = self.queries[0][0].__self__.table
        curr_threadID = threading.get_ident()
        latch = table.memory_manager.latches[table.name]
        latch.acquire()
        for item in table.lock_manager.threadID_to_locks[curr_threadID]:
            if type(item) is int:
                # Item is a base_id therefore it is a shared lock
                table.lock_manager.shared_locks[item].discard(curr_threadID)
            else: 
                # Item is an exclusive_locks dictionary
                item['Lock'].release()
                item['writerID'] = 0
        table.lock_manager.threadID_to_locks[curr_threadID] = list()
        latch.release()
        return
