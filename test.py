from __future__ import print_function

from dbh import DbHandler

create_table = '''
CREATE TABLE IF NOT EXISTS test_table (
testable_id SERIAL PRIMARY KEY,
"varchField" VARCHAR (255),
"boolField" BOOLEAN,
"intField" INTEGER
);
'''

drop_table = '''
DROP TABLE IF EXISTS test_table;
'''


def main():
    h = DbHandler()

    h.executeSql(drop_table)
    h.executeSql(create_table)

    for i in range(1, 101):
        last_id = h.insert_into('test_table', {
            'varchField': 'test_{}'.format(str(i)),
            'boolField': bool(i % 2),
            'intField': 100 + (100 - i)
        })

    if h.get_last_id_by_table_name('test_table') == last_id:
        print("TEST 1 PASSED!!!")
    else:
        print("TEST 1 FAILED!!!")


    where = {'varchField': ('like', 'test%'), 'boolField': False, 'intField': ('<=', 150)}

    rows_updated = h.update_table_set_where('test_table', set={'varchField': 'cool field'}, where=where)
    where['varchField'] = ('like', 'cool%')
    fieldset = h.select_from_where('test_table', where=where)
    rowscount = h.select_count_of('test_table', where=where)
    deleted_rows = h.delete_from_where('test_table', where=where)

    if rows_updated == len(fieldset) == rowscount == len(deleted_rows):
        print('TEST 2 PASSED!!!')
    else:
        print("TEST 2 FAILED!!!")

    rows_updated = h.update_table_set_where('test_table', set={'varchField': 'cool field'}, where=where)
    fieldset = h.select_from_where('test_table', where=where)
    rowscount = h.select_count_of('test_table', where=where)
    deleted_rows = h.delete_from_where('test_table', where=where)

    if rows_updated == len(fieldset) == rowscount == len(deleted_rows) == 0:
        print('TEST 3 PASSED!!!')
    else:
        print("TEST 3 FAILED!!!")

    h.delete_from_where('test_table', where={'intField': ('>=', 150)})
    num_rows = h.select_count_of('test_table')

    if num_rows == 25:
        print('TEST 4 PASSED!!!')
    else:
        print("TEST 4 FAILED!!!")

    h.executeSql(drop_table)

if __name__ == '__main__':
    main()

