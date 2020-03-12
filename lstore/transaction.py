from lstore.table import Table, Record
from lstore.index import Index
import threading
import sys

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
            print("query " + str(query.__name__) + " about to run")
            result, table, rid = query(*args)
            print("query " + str(query.__name__) + " finished")
            self.uncommittedQueries.append((query, rid, table))
            if result == False: # If the query has failed the transaction should abort
                print("aborted " + str(threading.get_ident()))
                self.uncommittedQueries.pop() #no need to "undo" the aborted transaction
                return self.abort(table)
        print("committed "+ str(threading.get_ident()))
        return self.commit(table)

    def abort(self, table):
        lock = RLock()
        #TODO: do roll-back and any other necessary operations
        for query in reversed(self.uncommittedQueries):
            fn_name = query[0].__name__
            if fn_name == 'update' or fn_name == 'increment':
                lock.acquire()
                print("found an update")
                table = query[2]
                rid = query[1]
                table.__undo_update__(rid)
                lock.release()
            else:
                print("didn't find an update, val is " + fn_name)
        table.release_locks()
        return False

    def commit(self, table):
        # TODO: commit to database
        while len(self.uncommittedQueries) != 0:
            query, key, index = self.uncommittedQueries.pop()
        table.release_locks()
        return True
