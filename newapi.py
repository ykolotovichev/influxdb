__author__ = 'Yury'

import requests
from time import time
from datetime import datetime
from random import random, randint
import gzip
from ykolutils.timing import Timer



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

    def write(self, dbname, points, retention_policy=None, precision=None, consistency=None, compress=False):

        url = '%s/%s' % (self.base_url, 'write')
        headers = {}
        params = {'db': dbname, 'u': self.user, 'p': self.password,
                  'rp': retention_policy, 'precision': precision, 'consistency': consistency}

        if compress:  # gzipping the request body
            headers = {'Content-encoding': 'gzip'}
            points = gzip.compress(points)

        try:
            r = requests.post(url, params=params, data=points, timeout=self.http_timeout, headers=headers)
            if r.status_code == 204:
                print('Points added.')
            else:
                print(r.status_code, r.text)
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


class SingleMeasurement:
    def __init__(self, name, fields, tags=None, timestamp=None, time_precision='n'):
        self.name = name
        self.tags = tags
        self.fields = fields
        self.time_precision = time_precision

        if isinstance(timestamp, str):
                self.timestamp = self._datetime_string_to_epoch(timestamp, time_precision)
        elif timestamp:
            self.timestamp = timestamp
        else:
            self.timestamp = ''

    @staticmethod
    def _datetime_string_to_epoch(datetime_string, precision):

        if datetime_string:
            patterns = ('%d.%m.%Y %H:%M:%S', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f')
            precision_multipliers = {'n': 1e+9, 'u': 1e+6, 'ms': 1e+3, 's': 1, 'm': 1/60, 'h': 1/3600}
            for pattern in patterns:
                try:
                    epoch = ((datetime.strptime(datetime_string, pattern) - datetime(1970, 1, 1)).total_seconds()) *\
                        precision_multipliers[precision]
                    return epoch
                except ValueError:
                    pass

    def to_bytes(self, decimals=3):
        # Making tags string representation
        tags_rep = ''
        if self.tags:
            tags_rep = ',' + ','.join(['%s=%s' % (k, v) for k, v in self.tags.items()])

        # Making fields string representation
        pattern = '%%s=%%.%df' % decimals
        fields_rep = ",".join([pattern % (k, v) for k, v in self.fields.items()])

        # Combining representations in a form of 'line protocol'
        if self.timestamp:
            str_rep = '%s%s %s %d\n' % (self.name, tags_rep, fields_rep, self.timestamp)
        else:
            str_rep = '%s%s %s\n' % (self.name, tags_rep, fields_rep)

        return str_rep.encode()  # converting str to bytes



class DummyMeasurements:

    def __init__(self, series,  npoints=1, decimals=3):
        self.series = series
        self.npoints = npoints
        self.decimals = decimals

    def generate(self):
        start_epoch = randint(1e+9, int(time()*1e+9))
        for i in range(0, self.npoints):
            m = SingleMeasurement(name=self.series,
                            fields={'X': random() * -720.0, 'Y': random() * 720.0, 'T': random() * 30.0},
                            timestamp=start_epoch + i)

            yield m.to_bytes(decimals=self.decimals)

    def to_bytes(self):
        points = b''.join(self.generate())
        return points


if __name__ == '__main__':

    # -----DB settings----------------- #

    #host = '46.101.128.140'
    host = '10.6.74.70'
    port = 8086

    dbname = 'testdb1'
    user = 'root'
    password = 'root'

    dbclient = InfluxDBClient(host=host, port=port, http_timeout=60)
    dbclient.create_database(dbname)

    dummies = DummyMeasurements('Tilt5', npoints=10000, decimals=4)

    with Timer():
        dbclient.write(dbname=dbname, points=dummies.generate(), compress=False, precision='n')



