__author__ = 'Yury A. Kolotovichev'

import requests
import logging
from exceptions import InfluxdbAPICodeMismatchError, InfluxdbAPIRequestError


class InfluxDBClient:
    def __init__(self, host, port, user=None, password=None, http_timeout=50, http_retries=3):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.http_timeout = http_timeout
        self.base_url = 'http://%s:%s' % (host, port)

        self.HTTPsession = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100, max_retries=http_retries)
        self.HTTPsession.mount('http://', adapter)

    def _request(self, url, method, expected_response_codes, headers=None, params=None, data=None):
        """Make a HTTP request"""

        try:
            r = self.HTTPsession.request(url=url,
                                         method=method,
                                         headers=headers,
                                         params=params,
                                         data=data,
                                         timeout=self.http_timeout)

            if r.status_code in expected_response_codes:
                logging.debug('HTTP request completed with expected code %d. %s' % (r.status_code, r.text))
                return r
            else:
                raise InfluxdbAPICodeMismatchError(r.text, r.status_code, expected_response_codes)

        except:
            raise InfluxdbAPIRequestError()

    def create_database(self, dbname):
        url = '%s/%s' % (self.base_url, 'query')
        params = {'q': 'CREATE DATABASE %s' % dbname, 'u': self.user, 'p': self.password}

        self._request(url=url, method='GET', params=params, expected_response_codes=(200, ))
        logging.info('Database <%s> created or already exists' % dbname)

        return True

    def drop_database(self, dbname):
        url = '%s/%s' % (self.base_url, 'query')
        params = {'q': 'DROP DATABASE %s' % dbname, 'u': self.user, 'p': self.password}

        self._request(url=url, method='GET', params=params, expected_response_codes=(200, ))
        logging.info('Database <%s> dropped' % dbname)

        return True

    def write(self, dbname, points, retention_policy=None, precision=None, consistency=None, gzipped=False):

        url = '%s/%s' % (self.base_url, 'write')
        headers = {}
        params = {'db': dbname, 'u': self.user, 'p': self.password,
                  'rp': retention_policy, 'precision': precision, 'consistency': consistency}

        if gzipped:  # request body gzipped
                headers = {'Content-encoding': 'gzip'}

        r = self._request(url=url,
                          method='POST',
                          params=params,
                          data=points,
                          headers=headers,
                          expected_response_codes=(204, 500))
        logging.info('Code %d: %s. Points added to database' % (r.status_code, r.text))

        return r

    def query(self, dbname, query):
        url = '%s/%s' % (self.base_url, 'query')
        params = {'db': dbname, 'q': query, 'u': self.user, 'p': self.password}

        r = self._request(url=url, method='GET', params=params, expected_response_codes=(200, ))
        logging.info('Code %d: %s. Data queried.' % (r.status_code, r.text))
        return r.json()

    def __repr__(self):
        return 'InfluxDBClient: %s' % self.base_url



if __name__ == '__main__':

    from ykolutils.timing import Timer
    from measurements import DummyPoints, Measurement

    logging.basicConfig(format='%(levelname)-8s [%(asctime)s]  %(message)s',
                        level=logging.DEBUG)

    # host = '46.101.128.140'
    host = '10.6.74.70'
    port = 8086

    dbname = 'unittestdb'
    user = 'root'
    password = 'root'

    dbclient = InfluxDBClient(host=host, port=port, http_timeout=2)
    dbclient.create_database(dbname)

    dbclient.drop_database(dbname)

    dbclient.create_database(dbname)

    # dummies = DummyPoints('Tilt5', npoints=100000, decimals=4, delta_seconds=600)
    # dummies.dump('dump.txt', compress=False)  # dumps points into text file
    # dummies.dump('dump.gz', compress=True)  # dumps points into compressed gzip file
    #
    single_point = Measurement(name='TestSeries1',
                               fields={'X': -100, 'Y': 720.0, 'T':  30.0},
                               timestamp='2015-12-30 10:36:43.567')

    # single Measurement instance
    with Timer('Single Measurement instance'):
        dbclient.write(dbname=dbname, points=single_point.to_bytes(), precision='n')
    #
    # single point as bytes
    # with Timer('Single point as bytes'):
    #     dbclient.write(dbname=dbname, points=b'Tilt5 X=-22.34,Y=653.8676,T=-4.1 1045513396921781872\n', precision='n')
    #
    # generator (chunked HTTP POST)
    # with Timer('Generator (chunked POST)'):
    #     dbclient.write(dbname=dbname, points=dummies, precision='n')
    #
    # bytearray (or bytes, dumped in-memory) (non-chunked, not compressed)
    # with Timer('Bytearray-not compressed'):
    #     dbclient.write(dbname=dbname, points=dummies.dump(), precision='n')

    # bytearray (or bytes, dumped in-memory) (non-chunked, compressed)
    # with Timer('Bytearray-compressed'):
    #     dbclient.write(dbname=dbname, points=dummies.dump(compress=True), gzipped=True, precision='n')
    #
    # from file (non-chunked)
    # with Timer('From text file'):
    #     dbclient.write(dbname=dbname, points=open('dump.txt', 'rb'), precision='n')
    #
    # from gzip file (non-chunked)
    # with Timer('From gzip file'):
    #     dbclient.write(dbname=dbname, points=open('dump.gz', 'rb'), gzipped=True, precision='n')










