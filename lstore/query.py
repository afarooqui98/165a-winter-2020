from lstore.table import Table, Record
from lstore.index import Index
from time import process_time
import struct
import lstore.config
import threading
from datetime import datetime
import threading

#if any new_columns are None type, give it the old_columns values
def compare_cols(old_columns, new_columns):
        for column_index in range(len(new_columns)):
            new_columns[column_index] = (old_columns[column_index] if new_columns[column_index] is None else new_columns[column_index])
        return new_columns

class Query:
    # Creates a Query object that can perform different queries on the specified table
    def __init__(self, table):
        self.table = table
        self.index = Index(self.table)
        pass

    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    def delete(self, key):
        self.table.__delete__(self.index.locate(key, self.table.key)[0])
        self.index.drop_index(key)
        return True, self.table

    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    def insert(self, *columns):
        base_rid = 0
        timestamp = process_time()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        indirection_index = 0
        key_index = self.table.key
        rid = self.table.base_RID
        columns = [indirection_index, rid, timestamp, base_rid] + list(columns)

        self.table.__insert__(columns) #table insert
        self.index.add_index(rid, columns[lstore.config.Offset:])

        self.table.base_RID += 1

        # Insert is not being tested so might not need this statement
        return True, self.table

    # Read a record with specified key
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    def select(self, key, column, query_columns):
        lock = threading.RLock()
        entries = self.index.locate(key, column)
        rids = []
        lock.acquire()
        for rid in entries:
            #2PL: acquire shared locks
            if len(entries) == 0:
                print("select returned false because it couldn't locate the key value")
                lock.release()
                return False, self.table, rid #return false to the transaction class if rid not found or abort because of locks
                # T F - thread has write lock, # F T - write lock is zero so can get read lock, T T - write lock held by someon else
            if self.table.acquire_read(rid) == False:
                print("select returned false because of locking error: tid is " + str(threading.get_ident()) + "outstanding_write rid is" + str(rid))
                lock.release()
                return False, self.table, rid
            else:
                pass
                #print("read has been acquired")

            rids.append(rid)
        lock.release()

        result = []
        for i in range(len(rids)):
            lock.acquire()
            result.append(self.table.__read__(rids[i], query_columns))
            lock.release()
        return result, self.table, None #TODO: inspect this later, might be a faulty way of returning the last value

    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    def update(self, key, *columns):
        lock = threading.RLock()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        indirection_index = 0
        rid = self.table.tail_RID

        old_columns = self.select(key, self.table.key, [1] * self.table.num_columns)[0][0].columns #get every column and compare to the new one: cumulative update
        new_columns = list(columns)

        old_rid = self.index.locate(key, self.table.key)[0] #get the IndexEntry of the old key val

        #2PL: acquire exlcusive locks
        lock.acquire()
        if self.table.acquire_write(rid) == False or self.table.acquire_write(old_rid) == False:
            lock.release()
            print("Failed write lock on thread " +  str(threading.get_ident()))
            return False, self.table, old_rid #return false to the transaction class if rid not found or abort
        lock.release()

        compared_cols = compare_cols(old_columns, new_columns)
        columns = [indirection_index, rid, timestamp, old_rid] + compared_cols
        lock.acquire()
        self.table.__update__(columns, old_rid) #add record to tail pages
        lock.release()

        old_indirection = self.table.__return_base_indirection__(old_rid) #base record, do not update index only insert

        lock.acquire()
        self.table.__update_indirection__(rid, old_indirection) #tail record gets base record's indirection index
        self.table.__update_indirection__(old_rid, rid) #base record's indirection column gets latest update RID
        lock.release()

        print(key)
        self.index.update_index(old_rid, compared_cols)
        print(key)

        print("size of index is : " + str(len(self.index.index_dict[0])))

        lock.acquire()
        self.table.tail_RID -= 1
        lock.release()

        return True, self.table, old_rid

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0
        for key in range(start_range, end_range + 1):
            temp_record = (self.select(key, self.table.key, [1] * self.table.num_columns)[0])
            if temp_record == False:
                return False, self.table
            if temp_record == -1 or len(temp_record) == 0:
                continue
            result += temp_record[0].columns[aggregate_column_index]

        return result, self.table


    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r, _, _ = self.select(key, self.table.key, [1] * self.table.num_columns)
        if r[0].rid is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[0].columns[column] + 1
            u, table, rid  = self.update(key, *updated_columns)
            return u, table, rid
        return False, self.table, None #TODO: check this!!!!!!
