from lstore.db import Database
from lstore.query import Query
from lstore.config import *
import sys
from time import process_time

from random import choice, randint, sample, seed

db = Database()
db.open('/ECS165')
# Student Id and 4 grades
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}
seed(3562901)

# PERFORMANCE TEST
time_0 = process_time()

for i in range(0, 1000):
    key = 92106429 + i
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
keys = sorted(list(records.keys()))
print("Insert finished")

# PERFORMANCE TEST
time_1 = process_time()
# print("Inserting took:  \t\t\t", time_1 - time_0)
insertTest = ("Inserting took:  \t\t\t", time_1 - time_0)

# PERFORMANCE TEST
time_2 = process_time()
for key in keys:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0][0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    # else:
    #     print('select on', key, ':', record)
print("Select finished")

# PERFORMANCE TEST
time_3 = process_time()
# print("Selecting took:  \t\t\t", time_3 - time_2)
selectTest = ("Select    took:  \t\t\t", time_3 - time_2)

# PERFORMANCE TEST
time_4 = process_time()

for _ in range(10):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        for i in range(1, grades_table.num_columns):
            value = randint(0, 20)
            updated_columns[i] = value
            original = records[key].copy()
            records[key][i] = value
            query.update(key, *updated_columns)
            record = query.select(key, 0, [1, 1, 1, 1, 1])[0][0]
            error = False
            for j, column in enumerate(record.columns):
                if column != records[key][j]:
                    error = True
            if error:
                print('update error on', original, 'and', updated_columns, ':', record.columns, ', correct:', records[key])
                # sys.exit(3)
            else:
                print('update on', original, 'and', updated_columns, ':', record.columns)
            updated_columns[i] = None
print("Update finished")

# PERFORMANCE TEST
time_5 = process_time()
# print("Update took:  \t\t\t", time_5 - time_4)
updateTest = ("Updating took:  \t\t\t", time_5 - time_4)

# PERFORMANCE TEST
time_6 = process_time()

for i in range(0, 100):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda key: records[key][0], keys[r[0]: r[1] + 1]))
    result = query.sum(keys[r[0]], keys[r[1]], 0)[0]
    if column_sum != result:
        print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
    # else:
    #     print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
print("Aggregate finished")

# PERFORMANCE TEST
time_7 = process_time()
# print("Aggregate took:  \t\t\t", time_7 - time_6)
aggregateTest = ("Aggregate took:  \t\t\t", time_7 - time_6)

print("----------------------------------------------------------------------")
print(insertTest)
print(selectTest)
print(updateTest)
print(aggregateTest)
print("----------------------------------------------------------------------")

db.close()
