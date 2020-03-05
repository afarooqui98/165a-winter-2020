from lstore.table import Table, Record
from lstore.index import Index
from time import process_time
import struct
import lstore.config
from datetime import datetime

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
    def delete(self, key):
        self.table.__delete__(self.index.locate(key, self.table.key)[0])
        self.index.drop_index(key)

    # Insert a record with specified columns
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

    # Read a record with specified key
    def select(self, key, column, query_columns):
        rids = self.index.locate(key, column)
        if rids == -1:
            return -1

        result = []
        for i in range(len(rids)):
            result.append(self.table.__read__(rids[i], query_columns))
        return result

    # Update a record with specified key and columns
    def update(self, key, *columns):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        indirection_index = 0
        rid = self.table.tail_RID

        old_columns = self.select(key, self.table.key, [1] * self.table.num_columns)[0].columns #get every column and compare to the new one: cumulative update
        new_columns = list(columns)

        old_rid = self.index.locate(key, self.table.key)[0]
        compared_cols = compare_cols(old_columns, new_columns)
        columns = [indirection_index, rid, timestamp, old_rid] + compared_cols
        self.table.__update__(columns, old_rid) #add record to tail pages

        old_indirection = self.table.__return_base_indirection__(old_rid) #base record, do not update index only insert

        self.table.__update_indirection__(rid, old_indirection) #tail record gets base record's indirection index
        self.table.__update_indirection__(old_rid, rid) #base record's indirection column gets latest update RID
        self.index.update_index(old_rid, compared_cols)
        self.table.tail_RID -= 1

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    """
    def sum(self, start_range, end_range, aggregate_column_index):
        result = 0
        for key in range(start_range, end_range + 1):
            temp_record = (self.select(key, self.table.key, [1] * self.table.num_columns))
            if temp_record == -1 or len(temp_record) == 0:
                continue
            result += temp_record[0].columns[aggregate_column_index]

        return result
