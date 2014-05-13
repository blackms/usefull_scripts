#!/usr/bin/env python

import threading
import Queue
import random
import string
import time
import argparse
import sys
import MySQLdb

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--username', help='Sql username.', action='store', dest='username', required=True)
parser.add_argument('-p', '--password', help='Sql password.', action='store', dest='password', required=True)
parser.add_argument('-H', '--hostname', help='Sql hostname.', action='store', dest='hostname', required=True)
parser.add_argument('-D', '--database', help='Sql database.', action='store', dest='database', required=True)
parser.add_argument('-t', '--maxthread', help='Number of concurrent threads.', action='store', dest='max_thread', required=False, type=str)
parser.add_argument('-N', '--numquery', help='Numbers of queries to execute.', action='store', dest='numbers', type=int)
parser.add_argument('-I', '--insert', help='Enable insert query test', action='store_true', dest='insert')
parser.add_argument('-S', '--select', help='Enable select query test', action='store_true', dest='select')
parser.add_argument('-P', '--prepare', help='Prepare the database to run tests.', action='store_true', dest='prepare')
p = parser.parse_args()

q_q = Queue.Queue()
t_list = []
query_lock = threading.Lock()
QUERIES = p.numbers
MAX_THREAD = 2 if p.max_thread is None else int(p.max_thread)


# except to use it later
def create_worker():
    t = Worker()
    t_list.append(t)
    t.setDaemon(True)
    t.start()
    return True


def insert_query(query=None, column_size=250, min_int=1000, max_int=10000000, data1=None, data2=None, num1=None, num2=None):
    # if query param is None I will use a premade query
    if query is None:
        insert = 'INSERT INTO table1 (data1, data2, numeric1, numeric2) VALUES (\'%s\', \'%s\', \'%s\', \'%s\');' % (data1, data2, num1, num2)
    else:
        insert = query
    return insert


# i will use it later
def select_query(query=None):
    if query is None:
        select = 'SELECT * FROM table1 where id=\'%s\'' % id_list[random.randint(0, 49999)]
    else:
        select = query
    return select


class Worker(threading.Thread):
    def __init__(self):
        super(Worker, self).__init__()
        self.stop = False
        self.db = MySQLdb.connect(user=p.username, passwd=p.password, host=p.hostname, db=p.database)
        self.cur = self.db.cursor()

    def run(self):
        while self.stop is False:
            if not q_q.empty():
                try:
                    query = q_q.get(True)
                    self.cur.execute(query)
                    # print(self.cur.fetchall())
                    q_q.task_done()
                except:
                    pass

    def __del__(self):
        self.db.commit()
        self.db.close()

if p.prepare is True:
    sql = MySQLdb.connect(user=p.username, passwd=p.password, host=p.hostname)
    cur = sql.cursor()
    cur.execute('CREATE DATABASE %s;' % (p.database))
    query = 'CREATE TABLE %s.table1 (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, data1 VARCHAR(256), data2 VARCHAR(256), numeric1 int, numeric2 int);' % (p.database)
    cur.execute(query)
    print('Prepare complete.')
    sys.exit(0)

if p.insert is True:
    rnd_data1 = []
    rnd_data2 = []
    rnd_numeric1 = []
    rnd_numeric2 = []
    print("Generating random...")
    for rnd in xrange(p.numbers+1):
        rnd_data1.append(''.join(random.choice(string.ascii_letters) for x in range(32)))
        rnd_data2.append(''.join(random.choice(string.ascii_letters) for x in range(32)))
        rnd_numeric1.append(random.randint(1000, 10000000))
        rnd_numeric2.append(random.randint(1000, 10000000))

for tn in xrange(MAX_THREAD):
    worker_t = Worker()
    t_list.append(worker_t)
    worker_t.setDaemon(True)
    worker_t.start()

conn = MySQLdb.connect(user=p.username, passwd=p.password, host=p.hostname)
cur = conn.cursor()
cur.execute('SELECT id FROM %s.table1' % (p.database))

id_list = []
if p.select is True:
    for tup in cur.fetchall():
        id_list.append(tup[0])

print('Starting processing: %s row with %s parallel threads...' % (p.numbers, MAX_THREAD))
print('Loading queries into queue...')

ts = time.time()
for i in xrange(QUERIES):
    if p.insert is True:
        query = insert_query(data1=rnd_data1[i], data2=rnd_data2[i], num1=rnd_numeric1[i], num2=rnd_numeric2[i])
    else:
        query = select_query()
    q_q.put(query)
print('Load Done.')
print('Waiting all queries to finish...')

while q_q.qsize() > 0:
    # sys.stdout.write('\rRemaning: %s queries to process.' % q_q.qsize())
    # sys.stdout.flush()
    time.sleep(0.1)
te = time.time()

for t in t_list:
    t.stop = True
# print('\nExecuted: %s, Execution time: %s sec.' % (executed, round((te-ts), 2)))
# print('Failed queries: %s' % (QUERIES-executed))
# print('Processed: %s queries/sec' % (QUERIES/round((te-ts), 2)))
print('Thread: %s, tr/s: %s' % (MAX_THREAD, QUERIES/round((te-ts), 2)))
