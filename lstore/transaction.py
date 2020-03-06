from template.table import Table, Record
from template.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.uncommittedQueries = []
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

    # This MUST return 0 if transaction is sucessful, else it must return 0
    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            query(*args)
        return 1
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        pass
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        pass
        # TODO: commit to database
        return True