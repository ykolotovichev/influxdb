__author__ = 'Yury Kolotovichev'

import requests


class InfluxDBClient:
    def __init__(self, host, port, user=None, password=None, http_timeout=50):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.http_timeout = http_timeout
        self.base_url = 'http://%s:%s' % (host, port)

    def create_database(self, dbname):
        url = '%s/%s' % (self.base_url, 'query')
        params = {'q': 'CREATE DATABASE %s' % dbname, 'u': self.user, 'p': self.password}

        try:
            r = requests.get(url, params=params, timeout=self.http_timeout)
            if r.status_code == 200:
                print('Database <%s> created or already exists' % dbname)
        except requests.exceptions.ConnectionError as err:
            print('Error using  HTTP request: %s' % err)

    def write(self, dbname, points, retention_policy=None, precision=None, consistency=None, gzipped=False):

        url = '%s/%s' % (self.base_url, 'write')
        headers = {}
        params = {'db': dbname, 'u': self.user, 'p': self.password,
                  'rp': retention_policy, 'precision': precision, 'consistency': consistency}

        if gzipped:  # request body gzipped
                headers = {'Content-encoding': 'gzip'}

        try:
            r = requests.post(url, params=params, data=points, timeout=self.http_timeout, headers=headers)
            if r.status_code == 204:
                print('Points added.')
            else:
                print(r.status_code, r.text)
            return r
        except requests.exceptions.ConnectionError as err:
            print('Error using  HTTP POST: %s' % err)
        except Exception as other_err:
            print('Other error: %s' % other_err)

    def query(self, dbname, query):
        url = '%s/%s' % (self.base_url, 'query')
        params = {'db': dbname, 'q': query, 'u': self.user, 'p': self.password}

        try:
            r = requests.get(url, params=params, timeout=self.http_timeout)
            if r.status_code == 200:
                print('Data queried.')
                return r.json()
        except requests.exceptions.ConnectionError as err:
            print('Error using  HTTP GET: %s' % err)

    def __repr__(self):
        return 'InfluxDBClient: %s' % self.base_url



if __name__ == '__main__':

    from ykolutils.timing import Timer
    from measurements import DummyPoints, Measurement
    import gzip



    #host = '46.101.128.140'
    host = '10.6.74.70'
    port = 8086

    dbname = 'unittestdb'
    user = 'root'
    password = 'root'

    dbclient = InfluxDBClient(host=host, port=port, http_timeout=60)
    dbclient.create_database(dbname)

    dummies = DummyPoints('Tilt5', npoints=1000, decimals=4)
    dummies.dump('dump.txt', compress=False)  # dumps points into text file
    dummies.dump('dump.gz', compress=True)  # dumps points into compressed gzip file

    single_point = Measurement(name='TestSeries1',
                               fields={'X': -100, 'Y': 720.0, 'T':  30.0},
                               timestamp='2015-12-30 10:36:43.567')

    # single Measurement instance
    with Timer('Single Measurement instance'):
        dbclient.write(dbname=dbname, points=single_point.to_bytes(), precision='n')

    # single point as bytes
    with Timer('Single point as bytes'):
        dbclient.write(dbname=dbname, points=b'Tilt5 X=-22.34,Y=653.8676,T=-4.1 1045513396921781872\n', precision='n')

    # generator (chunked HTTP POST)
    with Timer('Generator'):
        dbclient.write(dbname=dbname, points=dummies, precision='n')

    # bytearray (or bytes, dumped in-memory) (non-chunked, not compressed)
    with Timer('Bytearray-not compressed'):
        dbclient.write(dbname=dbname, points=dummies.dump(), precision='n')

    # bytearray (or bytes, dumped in-memory) (non-chunked, compressed)
    with Timer('Bytearray-compressed'):
        dbclient.write(dbname=dbname, points=gzip.compress(dummies.dump()), gzipped=True, precision='n')

    # from file (non-chunked)
    with Timer('From text file'):
        dbclient.write(dbname=dbname, points=open('dump.txt', 'rb'), precision='n')

    # from gzip file (non-chunked)
    with Timer('From gzip file'):
        dbclient.write(dbname=dbname, points=open('dump.gz', 'rb'), gzipped=True, precision='n')










