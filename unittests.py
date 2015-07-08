__author__ = 'Yury A. Kolotovichev'

import unittest
from measurements import Measurement, DummyPoints, Container
from influxdb import InfluxDBClient
import multiprocessing
from time import time
import logging


class DBClientTest(unittest.TestCase):

    # host = '46.101.128.140'
    host = '10.6.74.70'

    port = 8086

    dbname = 'unittestdb'
    user = 'root'
    password = 'root'

    series = 'Tilt'

    def setUp(self):  # Executes before every testcase

        self.single_point = Measurement(name=self.series,
                                        fields={'X': -100, 'Y': 720.0, 'T':  30.0},
                                        timestamp='2013-12-30 10:36:43.567')

        self.dummies = DummyPoints(self.series, npoints=1000, decimals=4, delta_seconds=600)

        self.dbclient = InfluxDBClient(host=self.host, port=self.port, http_timeout=600, http_retries=3)
        self.dbclient.create_database(self.dbname)

        self.startTime = time()

    def tearDown(self):  # Executes after every testcase
        t = time() - self.startTime
        print('Test <%s> executed in: %.3f seconds' % (self.id(), t))

        self.dbclient.drop_database(self.dbname)

    #@unittest.skip("Skipped")
    def test_InfluxDBClient_constructor(self):
        dbclient = InfluxDBClient(host=self.host, port=self.port, http_timeout=60, http_retries=3)
        print(dbclient)

    #@unittest.skip("Skipped")
    def test_create_database(self):
        self.dbclient.create_database(self.dbname)

    #@unittest.skip("Skipped")
    def test_write_single_Measurement(self):
        r = self.dbclient.write(dbname=self.dbname, points=self.single_point.to_bytes(), precision='n')
        self.assertEqual(r.status_code, 204)

    #@unittest.skip("Skipped")
    def test_query(self):
        self.dbclient.write(dbname=self.dbname, points=self.single_point.to_bytes(), precision='n')
        q = self.dbclient.query(dbname=self.dbname, query='SELECT * FROM %s' % self.series)
        print('Single point query: ', q)
        self.assertEqual(q['results'][0]['series'][0]['values'][0][1], 30)

    #@unittest.skip("Skipped")
    def test_write_in_memory_dumped_bytearray(self):
        # bytearray (or bytes, dumped in-memory) (non-chunked, not compressed)
        self.dbclient.write(dbname=self.dbname, points=self.dummies.dump(), precision='n')

    #@unittest.skip("Skipped")
    def test_write_container(self):

        measurement1 = Measurement(name='TestSeries1',
                                   fields={'X': -100, 'Y': 720.0, 'T':  30.0},
                                   timestamp='2013-12-30 10:36:43.567')

        measurement2 = Measurement(name='TestSeries1',
                                   fields={'X': -200, 'Y': 700.0, 'T':  25.0},
                                   timestamp='2013-12-30 10:37:43.567')

        measurement3 = Measurement(name='TestSeries1',
                                   fields={'X': -20, 'Y': 70.0, 'T':  15.0},
                                   timestamp='2013-12-30 10:38:43.567')

        container = Container(measurement1, measurement2)
        container.append(measurement3)
        print(container)

        self.dbclient.write(dbname=self.dbname, points=container.dump(decimals=4), precision='n')

    #@unittest.skip("Skipped")
    def test_chunked_write(self):
        # generator (chunked HTTP POST)
        r = self.dbclient.write(dbname=self.dbname, points=self.dummies, precision='n')
        self.assertEqual(r.status_code, 204)

    #@unittest.skip("Skipped")
    def test_write_gzipped_from_memory(self):
        # bytearray (or bytes, dumped in-memory) (non-chunked, compressed)
        r = self.dbclient.write(dbname=self.dbname, points=self.dummies.dump(compress=True), precision='n', gzipped=True)
        self.assertEqual(r.status_code, 204)

    #@unittest.skip("Skipped")
    def test_bulk_write_into_single_series(self):
        dummies = DummyPoints(self.series, npoints=5000, decimals=4, delta_seconds=600, opt='single_series')
        #print(dummies.dump())
        r = self.dbclient.write(dbname=self.dbname, points=dummies.dump(), precision='n')
        self.assertEqual(r.status_code, 204)

    #@unittest.skip("Skipped")
    def test_write_100000_points_in_5000_chunks_single_process(self):
        for chunk in range(0, 20):
            print('Sending chunk ', chunk)
            dummies = DummyPoints(self.series, npoints=5000, decimals=4, delta_seconds=600, opt='one_point_per_series')
            pts = dummies.dump()
            print('Sent bytes: ', len(pts))
            r = self.dbclient.write(dbname=self.dbname, points=dummies.dump(), precision='n')
            self.assertEqual(r.status_code, 204)

    def write_worker(self, npoints, nchunks):
        for chunk in range(0, nchunks):
            print('Sending chunk ', chunk)
            dummies = DummyPoints(self.series, npoints=npoints, decimals=4, delta_seconds=6000, opt='one_point_per_series')
            self.dbclient.write(dbname=self.dbname, points=dummies.dump(), precision='n')

    @unittest.skip("Skipped")
    def test_write_100000_points_in_10000_chunks_multiprocess(self):

        nworkers = 20
        npoints = 1000
        nchunks = 10

        workers = []
        for i in range(nworkers):
            p = multiprocessing.Process(target=self.write_worker, args=(), kwargs={'npoints': npoints,
                                                                                   'nchunks': nchunks})
            workers.append(p)
            p.start()

        for p in workers:
            p.join()







"""
def stats():

    q = dbclient.query(dbname, 'SELECT count(Y) from /Tilt_*/ where time < now() + 36500d')
    print(q)

    shard_size = get_remote_directory_size('/var/opt/influxdb/data', hostname=host, username=ssh_username,
                                           password=ssh_password, port=22)
    raft_size = get_remote_directory_size('/var/opt/influxdb/meta', hostname=host, username=ssh_username,
                                          password=ssh_password, port=22)


    try:
        points_in_series = []
        for item in q['results'][0]['name']:
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
"""

if __name__ == '__main__':
    # -----DB settings----------------- #
    #host = '46.101.128.140'
    host = '10.6.74.70'
    port = 8086

    dbname = 'unittestdb'
    user = 'root'
    password = 'root'

    series = 'Tilt'



    logging.basicConfig(format='%(levelname)-8s [%(asctime)s]  %(message)s',
                        level=logging.WARNING)

    unittest.main(verbosity=2)

    # dbclient = InfluxDBClient(host=host, port=port, http_timeout=60)
    # dbclient.create_database(dbname)
    # with Timer('Single sender'):
    #     dummies = DummyPoints(series, npoints=10000, decimals=3, delta_seconds=600)
    #     influx_sender(host, port, dbname, dummies.dump())
    #
    # dbclient.drop_database(dbname)





    """
    #logging.basicConfig(format='%(levelname)-8s [%(asctime)s]  %(message)s',
                        #level=logging.INFO)
    #multiprocessing.log_to_stderr()
    logger = multiprocessing.get_logger()
    logger.setLevel(logging.INFO)



    # -----SSH credentials------------- #
    ssh_username = 'guest'
    ssh_password = 'guest'


    # -----DB settings----------------- #
    #host = '46.101.128.140'
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
    NPOINTS = 1e+6  # Total number of points to write
    NSYS = 8  # Number of measurement1 systems(running in separate thread)
    CHUNK = 10000  # Max number of points in a single chunk

    STATS_LOG = 'stats.log'
    # -----PARAMETERS------------------ #

    N = math.ceil(NPOINTS / NSYS)  # points to send for a single measurement1 system
    CHUNK = N if CHUNK > NPOINTS / NSYS else CHUNK  # max CHUNK value should be limited

    with open(STATS_LOG, 'w') as f:
        f.write('Total number of points to write: %d\r\n' % NPOINTS)
        f.write('Number of writing threads: %d\r\n' % NSYS)
        f.write('Number of points in a single chunk: %d\r\n' % CHUNK)

    dbclient.create_database(dbname)
    writing_threads = write_in_threads()




    #q = dbclient.query(dbname, 'select count(T) from /Tilt_*/')
    #print('error' in q['results'][0])
    #print(q['results'][0]['name'][1]['values'][0][1])


    """



















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