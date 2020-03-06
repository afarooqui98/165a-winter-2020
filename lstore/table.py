from lstore.disk import *
from lstore.buffer import *
from time import time
import lstore.config
from pathlib import Path
import threading
import os
import sys
import pickle
import copy
import queue

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
BASE_RID_COLUMN = 3

#only return if there is a page directory file specified, only happens after the db has been closed
def read_page_directory(name):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/page_directory.pkl"
    if os.path.exists(file_name):
        with open(file_name, 'rb') as file:
            return pickle.load(file)
    else:
        return {} #just return empty dict

def write_page_directory(name, page_directory):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/page_directory.pkl"
    with open(file_name, "wb") as file:
        pickle.dump(page_directory, file)

def read_counters(name):
    counters = []
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/counters"
    if os.path.exists(file_name):
        with open(file_name, "rb") as file:
            for counter in range(6):
                counters.append(int.from_bytes(file.read(8), "big"))
        return counters

def write_counters(name, counters):
    file_name = os.getcwd() + lstore.config.DBName + "/" + name + "/counters"
    with open(file_name, "wb") as file:
        for counter in counters:
            file.write((counter).to_bytes(8, "big"))

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    :param base_RID/tail_RID    #start indexes for records for tail and base ranges
    :param base_range/tail_range #in memory representation of page storage
    """
    def __init__(self, name, num_columns, key, buffer_pool):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.sum = 0
        self.buffer = buffer_pool
        self.disk = None
        self.page_directory = {}
        self.page_directory_lock = False
        self.merge_queue = queue.Queue()

        self.base_RID = lstore.config.StartBaseRID
        self.tail_RID = lstore.config.StartTailRID
        self.base_RID_lock = 0
        self.tail_RID_lock = 0
        self.base_offset_counter = 0
        self.tail_offset_counter = 0

    def page_directory_wait():
        while self.page_directory_lock == True:
            print("waiting for page_directory to be unlocked by thread")

    #TODO: implement TPS calculation
    def __merge__(self, base_range_copy, tail_range_copies):
        for tail_range in reversed(tail_range_copies): #for every range reversed
            for record_index in range(lstore.config.PageEntries - 1, 0, -1): #for each record backwards, starting at index 511 (PageEntries - 1)

                tail_rid = tail_range[RID_COLUMN].read(record_index)
                base_rid_for_tail = tail_range[BASE_RID_COLUMN].read(record_index)

                _ , base_record_index = self.page_directory[base_rid_for_tail] #retrieve record index
                base_indirection = base_range_copy[INDIRECTION_COLUMN].read(base_record_index) #get indirection column for comparison

                if(base_indirection == tail_rid): #tail record is indeed the latest one
                    for column_index in range(lstore.config.Offset, lstore.config.Offset + self.num_columns): #for each column
                            tail_page_value = tail_range[column_index].read(record_index)
                            base_range_copy[column_index].inplace_update(base_record_index, tail_page_value)

                            # self.page_directory_lock = True
                            # self.page_directory.pop(tail_rid, None)
                            # self.page_directory_lock = False
                else:
                    pass
                    # self.page_directory_lock = True
                    # self.page_directory.pop(tail_rid, None) #in any case, remove this rid from the page directory
                    # self.page_directory_lock = False

        tps_value = tail_range_copies[len(tail_range_copies) - 1][RID_COLUMN].read(lstore.config.PageEntries - 1) #last record in last page is the TPS
        return (base_range_copy, tps_value)

    def __prepare_merge__(self, base_offset):
        base_range_copy = copy.deepcopy(self.buffer.fetch_range(self.name, base_offset)) #create a separate copy of the base range to put in the bg thread
        next_offset = self.disk.get_offset(self.name, 0, base_offset)
        self.buffer.unpin_range(self.name, base_offset) #update is finished, unpin
        tail_ranges = []

        counter = 0
        previous_offset = next_offset
        tail_offset = next_offset

        while counter != lstore.config.TailMergeLimit and tail_offset != 0:
            current_range = self.buffer.fetch_range(self.name, tail_offset)
            self.buffer.unpin_range(self.name, tail_offset) #since using for merge, we should pin it

            tail_ranges.append(current_range)
            previous_offset = tail_offset
            tail_offset = self.disk.get_offset(self.name, 0, previous_offset)
            counter += 1

        if counter < lstore.config.TailMergeLimit:
            # not enough pages to merge
            return

        consolidated_range, tps_value = self.__merge__(base_range_copy, tail_ranges) #initiate merge, return a consolidated range
        base_range = self.buffer.fetch_range(self.name, base_offset)

        #get metadata
        consolidated_range = base_range[:lstore.config.Offset] + consolidated_range[lstore.config.Offset:] #store the base metadata columns and the merged data columns
        self.buffer.unpin_range(self.name, base_offset) #unpin base_range after consolidation
        new_offset = (tail_offset if tail_offset == 0 else previous_offset) #either get the last tail_page's offset to point the base page to, or the previous offset if there is no next tail page
        for column_index in range(lstore.config.Offset + self.num_columns):
            consolidated_range[column_index].update_tps(tps_value) #update the tps in the consolidated pages before assignment
            self.disk.update_offset(self.name, column_index, base_offset, tail_offset) #update the offset of the ranges

        # for tail_range in tail_ranges:
        #     for column_index in range(lstore.config.Offset + self.num_columns):
        #         tail_range[column_index].empty_page() #empty out the tail pages

        while self.buffer.is_pinned(self.name, base_offset):
            print("pin count on range is " + str(self.buffer.get_pins(self.name, base_offset)))

        self.buffer.page_map[self.buffer.frame_map[base_offset]] = consolidated_range #update bufferpool

    def __add_physical_base_range__(self):
        if self.base_offset_counter < self.tail_offset_counter:
            self.base_offset_counter = self.tail_offset_counter + lstore.config.FilePageLength
        else:
            self.base_offset_counter += lstore.config.FilePageLength #increase offset after adding a range
        self.buffer.add_range(self.name, self.base_offset_counter)

    def __add_physical_tail_range__(self, previous_offset_counter):
        if self.tail_offset_counter < self.base_offset_counter:
            self.tail_offset_counter = self.base_offset_counter + lstore.config.FilePageLength
        else:
            self.tail_offset_counter += lstore.config.FilePageLength #increase offset after adding a range
        self.buffer.add_range(self.name, self.tail_offset_counter)

        for column_index in range(lstore.config.Offset + self.num_columns): #update all the offsets
            self.disk.update_offset(self.name, column_index, previous_offset_counter, self.tail_offset_counter) #update offset value i


    def __read__(self, RID, query_columns):
        # What the fick tail index and tails slots?
        tail_index = tail_slot_index = -1
        page_index, slot_index = self.page_directory[RID]

        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN] #index into the physical location
        self.buffer.unpin_range(self.name, page_index)

        current_page_tps = current_page.get_tps() #make sure the indirection column hasn't already been merged
        new_rid = current_page.read(slot_index)
        column_list = []
        key_val = -1
        if new_rid != 0 and (current_page_tps == 0 or new_rid < current_page_tps):
            tail_index, tail_slot_index = self.page_directory[new_rid] #store values from tail record
            current_tail_range = self.buffer.fetch_range(self.name, tail_index)
            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    #TODO TF is this shit, does it acctually give the key val
                    key_val = query_columns[column_index - lstore.config.Offset]
                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_tail_page = current_tail_range[column_index] #get tail page from
                    column_val = current_tail_page.read(tail_slot_index)
                    column_list.append(column_val)
            self.buffer.unpin_range(self.name, tail_index) #unpin at end of transaction
        else:
            current_base_range = self.buffer.fetch_range(self.name, page_index)
            for column_index in range(lstore.config.Offset, self.num_columns + lstore.config.Offset):
                if column_index == self.key + lstore.config.Offset:
                    key_val = query_columns[column_index - lstore.config.Offset] #subtract offset for the param columns

                if query_columns[column_index - lstore.config.Offset] == 1:
                    current_base_page = current_base_range[column_index]
                    column_val = current_base_page.read(slot_index)
                    column_list.append(column_val)
            self.buffer.unpin_range(self.name, page_index) #unpin at end of transaction

        # check indir column record
        # update page and slot index based on if there is one or nah
        return Record(RID, key_val, column_list) #return proper record, or -1 on key_val not found

    def __insert__(self, columns):
        #returning any page in range will give proper size
        current_range = self.buffer.fetch_range(self.name, self.base_offset_counter)[0]
        self.buffer.unpin_range(self.name, self.base_offset_counter) #unpin after getting value
        if not current_range.has_capacity(): #if latest slot index is -1, need to add another range
            self.__add_physical_base_range__()

        page_index = self.base_offset_counter
        current_base_range = self.buffer.fetch_range(self.name, page_index)
        for column_index in range(self.num_columns + lstore.config.Offset):
            current_base_page = current_base_range[column_index]
            slot_index = current_base_page.write(columns[column_index])
        self.page_directory[columns[RID_COLUMN]] = (page_index, slot_index) #on successful write, store to page directory
        self.buffer.unpin_range(self.name, page_index) #unpin at the end of transaction

    #in place update of the indirection entry.
    def __update_indirection__(self, old_RID, new_RID):
        page_index, slot_index = self.page_directory[old_RID]
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN]
        current_page.inplace_update(slot_index, new_RID)
        self.buffer.unpin_range(self.name, page_index) #unpin after inplace update

    # Set base page entry RID to 0 to invalidate it
    def __delete__ (self, RID):
        page_index, slot_index = self.page_directory[RID]
        current_page = self.buffer.fetch_range(self.name, page_index)[RID_COLUMN]
        current_page.inplace_update(slot_index, 0)
        self.buffer.unpin_range(self.name, page_index) #unpin after inplace update

    def __return_base_indirection__(self, RID):
        page_index, slot_index = self.page_directory[RID]
        current_page = self.buffer.fetch_range(self.name, page_index)[INDIRECTION_COLUMN]
        indirection_index = current_page.read(slot_index)
        self.buffer.unpin_range(self.name, page_index) #unpin after reading indirection
        return indirection_index

    def __traverse_tail__(self, page_index):
        counter = 0
        tail_offset = self.disk.get_offset(self.name, 0, page_index) #tail pointer at the specified base page in disk
        prev_tail = page_index
        while tail_offset != 0:
            prev_tail = tail_offset
            tail_offset = self.disk.get_offset(self.name, 0, prev_tail)
            counter += 1

        tail_offset = prev_tail
        return tail_offset, counter

    def __update__(self, columns, base_rid):
        base_offset, _ = self.page_directory[base_rid]

        current_tail = None
        previous_offset, num_traversed = self.__traverse_tail__(base_offset)
        page_offset = previous_offset
        if previous_offset == base_offset: #if there is no tail page for the base page
            self.__add_physical_tail_range__(previous_offset)
            page_offset = self.tail_offset_counter

        current_tail = self.buffer.fetch_range(self.name, page_offset)[0]
        self.buffer.unpin_range(self.name, page_offset)  #just needed to read this once, unpin right after
        if not current_tail.has_capacity(): #if the latest tail page is full
            self.__add_physical_tail_range__(previous_offset)
            page_offset = self.tail_offset_counter #add the new range and update the tail offsets accordingly

            self.buffer.unpin_range(self.name, base_offset)

            if (num_traversed >= lstore.config.TailMergeLimit) and (self.buffer.fetch_range(self.name, base_offset)[0].has_capacity() == False): # maybe should be >=, check to see if the base page is full
                self.buffer.unpin_range(self.name, base_offset)
                self.merge_queue.put(base_offset) #add page offset to the queue
                if len(threading.enumerate()) == 1: #TODO: check the name of the current thread, needs to make sure that merge isn't in the pool
                    thread = threading.Thread(name = "merge_thread", target = self.__prepare_merge__, args = [self.merge_queue.get()]) # needs to be called in a threaded way, pass through the deep copy of the base range
                    thread.start()

        current_tail_range = self.buffer.fetch_range(self.name, page_offset)
        for column_index in range(self.num_columns + lstore.config.Offset):
            current_tail_page = current_tail_range[column_index]
            slot_index = current_tail_page.write(columns[column_index])
        self.page_directory[columns[RID_COLUMN]] = (page_offset, slot_index) #on successful write, store to page directory
        self.buffer.unpin_range(self.name, page_offset) #update is finished, unpin
