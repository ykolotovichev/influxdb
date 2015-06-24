__author__ = 'Yury'
from influxdb import InfluxDBClient, DummyMeasurements
from random import random
import threading
import multiprocessing
import math
from ykolutils.timing import Timer
from ykolutils.monitoring import get_remote_directory_size
from time import time, strftime, localtime, sleep

'''
def make_point(series, fields, timestamp=None, precision='u', tags=None):
    if timestamp:
        point = {'name': series,
                 'time': timestamp,
                 'precision': precision,
                 'fields': fields,
                 'tags': tags
                 }
    else:
        point = {'name': series,
                 'fields': fields,
                 'tags': tags
                 }
    return point
'''

def chunksize_gen(n, chunk):  # generates chunks sizes
    """
     n - number of points to be send by each system
     chunk - maximum number of points in single JSON
    """

    for i in range(int(n / chunk + math.ceil((n % chunk) / chunk))):
        n -= chunk
        chunksize = chunk if n >= 0 else n+chunk
        yield chunksize



def send_to_influx(series, chunk_sizes, name):
    with Timer(name):

        for parcel_num, chunk_size in enumerate(chunk_sizes):

            #points = ''.join(points_generator(series, chunk_size, decimals=3))

            print('%s: Sending parsel %d' % (name, int(parcel_num+1)))
            try:
                dummies = DummyMeasurements(series, chunk_size, decimals=3)
                dbclient.write(dbname, dummies.generate(), compress=False)
                print('%s: Points sent: %d' % (name, chunk_size))
            except:
                print('%s: Error writing data.' % name)


def write_in_threads():

    print('%d points will be sent by every of %d measurement system. TOTAL: %d ' % (N, NSYS, N*NSYS))
    print('Points per JSON parcel: ', CHUNK)

    threads = []
    for i in range(NSYS):
        sizes = chunksize_gen(N, CHUNK)
        print(type(sizes))
        t = threading.Thread(target=send_to_influx, args=(), kwargs={'series': 'Tilt_' + str(i+1),
                                                                     'chunk_sizes': sizes,
                                                                     'name': 'Thread ' + str(i+1)})
        threads.append(t)
        t.start()
    return threads


def stats():

    q = dbclient.query(dbname, 'SELECT count(Y) from /Tilt_*/ where time < now() + 36500d')
    print(q)

    shard_size = get_remote_directory_size('/var/opt/influxdb/data', hostname=host, username=ssh_username,
                                           password=ssh_password, port=22)
    raft_size = get_remote_directory_size('/var/opt/influxdb/meta', hostname=host, username=ssh_username,
                                          password=ssh_password, port=22)


    try:
        points_in_series = []
        for item in q['results'][0]['series']:
            points_in_series.append(item['values'][0][1])
        points_in_db = sum(points_in_series)
    except Exception:
        points_in_db = 0
        print('Error receiving stats')

    time_str = strftime('%Y-%m-%d %H:%M:%S', localtime(time()))

    num_of_writing_threads_alive = threading.active_count() - 1

    with open(STATS_LOG, 'a') as f:
        f.write('%s, %d, %f, %f, %d\r\n' % (time_str,
                                            points_in_db,
                                            shard_size,
                                            raft_size,
                                            num_of_writing_threads_alive))

    return time_str, points_in_db, shard_size, raft_size, num_of_writing_threads_alive


def gather_stats():

    while True:
        try:
            st = stats()
            print('<%s> points: %d, shard size: %f MB, raft size %f MB, alive threads: %d' % st)
            sleep(2)
        except Exception:
            print('Data bug')


if __name__ == '__main__':
    # -----SSH credentials------------- #
    ssh_username = 'guest'
    ssh_password = 'guest'


    # -----DB settings----------------- #
    host = '10.6.74.70'
    port = 8086

    dbname = 'testdb2'
    user = 'root'
    password = 'root'

    http_timeout = 60

    dbclient = InfluxDBClient(host=host,
                              port=port,
                              user=user,
                              password=password,
                              http_timeout=http_timeout)

    # -----DB settings----------------- #

    # -----PARAMETERS------------------ #
    NPOINTS = 5e+6  # Total number of points to write
    NSYS = 10  # Number of measurement systems(running in separate thread)
    CHUNK = 10000  # Max number of points in a single chunk

    STATS_LOG = 'stats.log'
    # -----PARAMETERS------------------ #

    N = math.ceil(NPOINTS / NSYS)  # points to send for a single measurement system
    CHUNK = N if CHUNK > NPOINTS / NSYS else CHUNK  # max CHUNK value should be limited

    with open(STATS_LOG, 'w') as f:
        f.write('Total number of points to write: %d\r\n' % NPOINTS)
        f.write('Number of writing threads: %d\r\n' % NSYS)
        f.write('Number of points in a single chunk: %d\r\n' % CHUNK)

    dbclient.create_database(dbname)
    stats()
    writing_threads = write_in_threads()
    gather_stats()



    #q = dbclient.query(dbname, 'select count(T) from /Tilt_*/')
    #print('error' in q['results'][0])
    #print(q['results'][0]['series'][1]['values'][0][1])






















'''
    n=[]
    k = N
    for i in range(NParsels):
        k -= CHUNK
        if k >= 0:
            n.append(CHUNK)
        else:
            n.append(k+CHUNK)
    print(n)

'''






    #dbclient.create_database('testdb4')

    #write_in_threads(npoints_in_json, single_series=False)








'''
#-----------------------------------------------------------#
js2 = {'database': 'testdb4',
       'retentionPolicy': 'default',
       'points': [
                  {'name': 'Lakhta.BF.Tilt_1',
                   'time': int(time()*1000000+1),
                   'precision': 'u',
                   'fields': {'X': random() * 720, 'Y': random() * 720, 'T': random() * 30},
                   'tags': {}
                   },
                  {'name': 'Lakhta.BF.Tilt_1',
                   'time': int(time()*1000000+2),
                   'precision': 'u',
                   'fields': {'X': random() * 720, 'Y': random() * 720, 'T': random() * 30},
                   'tags': {}
                   },
                   {'name': 'Lakhta.BF.Tilt_1',
                    'time': int(time()*1000000+3),
                    'precision': 'u',
                    'fields': {'X': random() * 720, 'Y': random() * 720, 'T': random() * 30},
                    'tags': {}
                   },
                  ]
       }
#'time': int(time()*1000000+1000000*30),
print(js2['points'][0]['fields']['X'])
print(js2['points'][1]['fields']['X'])
print(js2['points'][2]['fields']['X'])

dbclient.write(js2)
#-----------------------------------------------------------#
'''

'''
#-----------------------------------------------------------#

js = list()

for i in range(0, 10000):
    js.append({'name': 'Lakhta.BF.Tilt_1',
                   'time': int(time()*1000000+1),
                   'precision': 'u',
                   'fields': {'X': random() * 720, 'Y': random() * 720, 'T': random() * 30},
                   'tags': {}
                   })
    print(js[i]['time'], js[i]['fields']['X'])
    sleep(0.001)

print('json ready', len(js))

print(json.dumps(js))

dbclient.write_points(js)
print('Json added')

result = dbclient.query('select count(X) from "Lakhta.BF.Tilt_1"')
print('No of measurements: ', result)

#-----------------------------------------------------------#
'''



#, sort_keys=True, indent=4, separators=(',', ': ')

'''
class TiltmeterMeasurement(SeriesHelper):
    class Meta:
        client = dbclient
        series_name = 'Lakhta.BF.Tilt'
        fields = ['X', 'Y', 'T']
        tags = ['tiltmeter_num', 'type']

#for i in range(1, 1000):
#    point = TiltmeterMeasurement(tiltmeter_num='1', X=random() * 720, Y=random() * 720, T=random() * 25, type='Horizont Ind3')
#    point = TiltmeterMeasurement(tiltmeter_num='2', X=random() * 720, Y=random() * 720, T=random() * 25, type='Nivel')

#    TiltmeterMeasurement.commit()
#print(TiltmeterMeasurement._json_body_())

print(dbclient)
'''