__author__ = 'Yury Kolotovichev'

import requests
import gzip
from ykolutils.timing import Timer

from measurements import Dummy


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





if __name__ == '__main__':



    #host = '46.101.128.140'
    host = '10.6.74.70'
    port = 8086

    dbname = 'unittestdb'
    user = 'root'
    password = 'root'

    dbclient = InfluxDBClient(host=host, port=port, http_timeout=60)
    dbclient.create_database(dbname)

    dummies = Dummy('Tilt5', npoints=1000, decimals=4)

    with Timer():
        dbclient.write(dbname=dbname, points=dummies.generate(), compress=False, precision='n')

    with Timer():
        dbclient.write(dbname=dbname, points=dummies.to_bytes(), compress=False, precision='n')



