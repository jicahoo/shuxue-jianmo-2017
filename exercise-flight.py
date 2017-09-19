# -*- coding: utf-8 -*-

from munkres import Munkres

# SELECT * FROM schedules WHERE  type = '9' and ((from_airport = 'OVS' and start_time > '18:00' and start_time < '21:100')
#                               or (to_airport = 'OVS' and start_time > '18:00' and end_time < '21:00'))

# SELECT LIMIT GROUP 2 (*) order start_time FROM schedules LEFT JOIN PreivousSQLView ON shedulles.tail_number = PreviousSQLView.tail_number GROUP BY schedules.tail_number.

import xlrd
import sqlite3


def csv_from_excel():
    result = []
    wb = xlrd.open_workbook('Schedules.xlsx', encoding_override="utf-8")
    sh = wb.sheet_by_name('Sheet1')
    for rownum in xrange(sh.nrows):
        result.append(sh.row_values(rownum)[:7])
    columns = ['flight_no', 'start_time', 'end_time', 'from_airport', 'to_airport', 'aircraft_type',
               'aircraft_tail_number']
    result[0] = columns
    new_result = []
    new_result.append(result[0])
    for x in result[1:]:
        new_record = []
        for y in x:
            if type(y) is float:
                new_record.append(int(y))
            else:
                new_record.append(y)
        new_result.append(new_record)
    return new_result


def create_table(cur, table_name, columns, types, data):
    cur.execute("CREATE TABLE %s (%s);" %
                (table_name,
                 ",".join([' '.join(x) for x in zip(columns, types)])
                 )
                )

    cur.executemany(
        "INSERT INTO %s (%s) VALUES (%s);" % (table_name, ",".join(columns), ",".join(['?'] * len(columns))), data)


def column_max(cur, table_name, column_name):
    sql = "select max(%s) from %s" % (column_name, table_name)
    cur.execute(sql)
    ret = []
    for row in cur:
        ret.append(row)
    return ret[0][0]


SQL = '''SELECT * FROM schedules WHERE  aircraft_type = '9' and 
                                            (  
                                                (from_airport = 'OVS' and start_time%(3600*24) > 18*3600 and start_time%(3600*24) < 21*3600) or 
                                                (to_airport = 'OVS' and start_time%(3600*24) > 18*3600 and end_time%(3600*24) < 21*3600)
                                            ) 
                                            order by start_time'''


# SQL = '''SELECT * FROM schedules WHERE  aircraft_type = '9' and (from_airport = 'OVS' and start_time%(3600*24) > 18*3600)'''

def filter_schedules(cur, sql):
    cur.execute(sql)
    for row in cur:
        print row


SCHEDULES_TABLE = 'schedules'
if __name__ == '__main__':
    res = csv_from_excel()
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    types = ['integer', 'integer', 'integer', 'text', 'text', 'text', 'text']
    create_table(cur, SCHEDULES_TABLE, res[0], types, res[1:])
    filter_schedules(cur, SQL)
    con.commit()
    con.close()
