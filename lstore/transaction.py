from lstore.table import Table, Record
from lstore.index import Index
import threading

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.uncommittedQueries = [] #tuples of the key value and the index structure pointer
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

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        print("in run function")
        for query, args in self.queries:
            print("query about to run")
            result, index = query(*args)
            self.uncommittedQueries.append((args[0], index))
            if result == False: # If the query has failed the transaction should abort
                print("aborted")
                self.uncommittedQueries.pop() #no need to "undo" the aborted transaction
                return self.abort()
        print("committed")
        return self.commit()

    def abort(self, index):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self, index):
        # TODO: commit to database
        while len(self.uncommittedQueries) != 0:
            key, index = self.uncommittedQueries.pop()
            self.release_locks(key, index)
        return True

    def release_locks(self, key, index):
        index.release_locks()
        pass