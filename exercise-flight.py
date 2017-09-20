# -*- coding: utf-8 -*-
import sys
from munkres import Munkres

# SELECT * FROM schedules WHERE  type = '9' and ((from_airport = 'OVS' and start_time > '18:00' and start_time < '21:100')
#                               or (to_airport = 'OVS' and start_time > '18:00' and end_time < '21:00'))

# SELECT LIMIT GROUP 2 (*) order start_time FROM schedules LEFT JOIN PreivousSQLView ON shedulles.tail_number = PreviousSQLView.tail_number GROUP BY schedules.tail_number.

import xlrd
import sqlite3


def parse_excel():
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


def import_to_database(cur, table_name, columns, types, data):
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
    result = []
    for row in cur:
        result.append(row)

    return [list(x) for x in result]


def query(cur, sql):
    cur.execute(sql)
    result = []
    for row in cur:
        result.append(list(row))
    return result


def count_flights(cur, start_time, airport, is_to_airport=False):
    sql = ''
    if is_to_airport:
        sql = "SELECT count(*) FROM schedules WHERE end_time = %s and to_airport = '%s'" % (start_time, airport)
    else:
        sql = "SELECT count(*) FROM schedules WHERE start_time = %s and from_airport = '%s'" % (start_time, airport)
    res = query(cur, sql)
    return res[0][0]

def update_t2(cur, t2a, t2b):
    new_ovs_start_flights = []
    nine_pm = 1461358800
    for idx, ovs_start in enumerate(t2a):
        new_ovs_start = [x for x in ovs_start]
        new_ovs_start_flights.append(new_ovs_start)
        for i in range(0, 300, 10):
            if count_flights(cur, nine_pm + i * 60, 'ovs') + 1 <= 5:
                new_ovs_start_flights[idx][1] = nine_pm + i * 60
                break

    new_ovs_end_flights = []
    for idx, ovs_end in enumerate(t2b):
        new_ovs_end = [x for x in ovs_end]
        new_ovs_end_flights.append(new_ovs_end)
        for i in range(0, 300, 10):
            if count_flights(cur, nine_pm + i * 60, 'ovs', True) + 1 <= 5:
                duration = new_ovs_end_flights[idx][2] - new_ovs_end_flights[idx][1]
                new_ovs_end_flights[idx][2] = nine_pm + i * 60
                new_ovs_end_flights[idx][1] = nine_pm + i * 60 - duration
                break
    return (new_ovs_start_flights, new_ovs_end_flights)


SCHEDULES_TABLE = 'schedules'
UPDATE_TIME_SQL = ''' UPDATE schedules
          SET start_time = ? ,
              end_time = ?
          WHERE flight_no = ?'''

if __name__ == '__main__':
    res = parse_excel()
    con = sqlite3.connect(":memory:")
    cur = con.cursor()
    types = ['integer', 'integer', 'integer', 'text', 'text', 'text', 'text']
    import_to_database(cur, SCHEDULES_TABLE, res[0], types, res[1:])
    # Step 1
    t1 = filter_schedules(cur, SQL)
    # print 'Step 1:' + '-' * 100
    # for x in delay_flight:
    #     print x

    t2b = []

    t2a = []
    for record in t1:
        if record[3] == 'OVS':
            t2a.append(record)
        if record[4] == 'OVS':
            t2b.append(record)


    def ovs_start_cmp(a, b):
        return a[1] - b[1]


    t2a.sort(ovs_start_cmp)


    def ovs_end_cmp(a, b):
        return a[2] - b[2]


    t2b.sort(ovs_end_cmp)

    # Compute t3a, t3b
    t3a = []
    for record in t2a:
        tail_number = record[6]
        start_time = record[2]
        sql = "SELECT * FROM %s WHERE aircraft_tail_number = '%s' and start_time > %s order by start_time limit 1" % (
            SCHEDULES_TABLE, tail_number, start_time)
        # print '-' * 100
        res = query(cur, sql)
        # print res
        t3a.append(list(res[0]))


    def t3_cmp(a, b):
        return a[1] - b[1]


    t3a.sort(t3_cmp)
    for t3a_r in t3a:
        print t3a_r

    t3b = []
    for record in t2b:
        tail_number = record[6]
        start_time = record[2]
        sql = "SELECT * FROM %s WHERE aircraft_tail_number = '%s' and start_time > %s order by start_time limit 1" % (
            SCHEDULES_TABLE, tail_number, start_time)
        # print '-' * 100
        res = query(cur, sql)
        # print res
        t3b.append(list(res[0]))
    t3b.sort(t3_cmp)
    print '-' * 100
    for t3b_r in t3b:
        print t3b_r

    t3 = t3a + t3b
    t3.sort(t3_cmp)
    print 't3 - ' * 100
    updated = update_t2(cur, t2a, t2b)
    t2a = updated[0]
    t2b = updated[1]
    for r in (t2a+t2b):
        cur.execute(UPDATE_TIME_SQL, (r[1],r[2],r[0]))

    i = 0
    while len(t3) != 0:
        t3_r = t3[i]
        i += 1
        from_airport = t3_r[3]
        t4 = query(cur,
                   "select * from schedules where from_airport = '%s' and aircraft_type ='9' order by start_time" % (
                   from_airport))
        t3_tail_number = t3_r[-1]

        # temp = query(cur,
        #              "select * from schedules where aircraft_type ='9' and aircraft_tail_number = '%s' order by start_time" % (
        #              t3_tail_number))
        t2_r = None
        for t2_r_i in (t2a+t2b):
            # print 'cmp: %s,%s' % (t2_r_i[-1], t3_tail_number)
            if t2_r_i[-1] == t3_tail_number:
                t2_r = t2_r_i
                break
        #
        # previous = None
        # for my_r in temp:
        #     if my_r[0] == t3_r[0]:
        #         break
        #     previous = my_r
        # if previous is None:
        #     print t3_r
        #     print '-' * 100
        #     for x in temp:
        #         print x
        #     exit(1)

        # t2_r = previous
        t5 = [[r[1], r[-2]] for r in t4]
        goal = t2_r[2] + 45 * 60
        idx = None
        for i, r in enumerate(t4):
            if r[0] == t3_r[0]:
                idx = i
                break
        t5[idx][0] = goal


        def t5_cmp(a, b):
            return a[0] - b[0]


        t5.sort(t5_cmp)

        matrix = []
        n = len(t4)
        print 'n' + str(n)

        m = len(t5)
        print 'm' + str(m)
        for i in range(n):
            matrix.append([sys.maxint] * m)

        for i, t4_r in enumerate(t4):
            for j, t5_r in enumerate(t5):
                if j >= i:
                    delta = t5_r[0] - t4_r[1]
                    if delta <= 5 * 60 * 60:
                        matrix[i][j] = delta
        for x in matrix:
            print ','.join([str(y) for y in x])
        m = Munkres()
        indexes = m.compute(matrix)
        print indexes
        new_t4 = []
        for t in indexes:
            flight_idx = t[0]
            aircraft_idx = t[1]
            flight_r = t4[flight_idx]
            aircraft_r = t5[aircraft_idx]
            flight_r[1] = aircraft_r[0]
            new_t4.append(flight_r)

        for new_t4_r in new_t4:
            cur.execute(UPDATE_TIME_SQL, (new_t4_r[1], new_t4_r[2], new_t4_r[0]))
            s = len(t3)
            target_i = 0
            for target_i in range(len(t3)):
                x = t3[target_i]
                target_i += 1
                if new_t4_r[1] < x[1]:
                    break
            t3.insert(target_i - 1, new_t4_r)

    # Compute t4

    for delay in t1:
        # print '--' * 100
        flights = query(cur,
                        "select * from schedules where from_airport = '%s' and aircraft_type ='9' order by start_time" % (
                        delay[3]))
        # for flight in flights:
        #     print flight

    # Step 2
    for record in t1:
        tail_number = record[6]
        start_time = record[2]
        sql = "SELECT * FROM %s WHERE aircraft_tail_number = '%s' and start_time > %s order by start_time limit 1" % (
            SCHEDULES_TABLE, tail_number, start_time)
        # print '-' * 100
        res = query(cur, sql)
        # print res

    con.commit()
    con.close()
