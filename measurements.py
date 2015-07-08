__author__ = 'Yury A. Kolotovichev'

from time import time
from random import random, randint
from datetime import datetime
import gzip
import logging






# line protocol
class Measurement:
    """
    Base measurement class to simplify Influxdb line protocol implementation
    """

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
        """
        Private method to convert datetime string to Unix epoch
        """

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

    def to_string(self, decimals=3):
        """
        Influxdb line protocol string representation
        """

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

        return str_rep

    def to_bytes(self, decimals=3):
        """
        Influxdb line protocol in a form of bytes
        """
        return self.to_string(decimals).encode()

    def __repr__(self):
        return self.to_string()

    def __str__(self):
        return self.to_string().strip()


class Container:

    def __init__(self, *measurements):
        self.points = []
        self.points = [measurement for measurement in measurements]

    def append(self, *measurements):
        for measurement in measurements:
            self.points.append(measurement)

    def dump(self, decimals=3):
        array = bytearray()
        for measurement in self.points:
            array.extend(measurement.to_bytes(decimals=decimals))
        return array

    def __iter__(self):
        return iter(self.points)

    def __str__(self):
        return 'Contains: %d Measurements' % len(self.points)



class DummyPoints:
    """
    Dummy points generator. Useful for unit testing.
    """

    def __init__(self, name,  npoints=1, decimals=3, delta_seconds=1, opt='one_point_per_series'):
        """
        :param name: time series name
        :param npoints: number of points to generate
        :param decimals: number of decimals in line protocol representation
        :param delta_seconds: time delta (in seconds) of sequential points
        :return:
        """
        self.name = name
        self.npoints = npoints
        self.decimals = decimals
        self.delta_seconds = delta_seconds
        self.start_epoch = randint(1e+9, int(time()*1e+9))
        self.opt = opt

    def generate(self, opt):
        """
        Generates dummy points in form of Influxdb line protocol (bytes)
        :return: point generator
        """

        if opt == 'single_series':
            for i in range(0, self.npoints):
                ts = self.start_epoch + i*1e+9*self.delta_seconds
                m = Measurement(name=self.name,
                                fields={'X': random() * -720.0, 'Y': random() * 720.0, 'T': random() * 30.0},
                                timestamp=ts)
                m.to_bytes(decimals=self.decimals)

                yield m.to_bytes(decimals=self.decimals)
            self.start_epoch = ts + self.delta_seconds * 1e+9  # update start_epoch to make points consequent

        elif opt == 'one_point_per_series':
            start_epoch = int(time())*1e+9
            for i in range(0, self.npoints):
                m = Measurement(name='%s_%d' % (self.name, i),
                                fields={'X': random() * -720.0, 'Y': random() * 720.0, 'T': random() * 30.0},
                                timestamp=start_epoch)
                m.to_bytes(decimals=self.decimals)

                yield m.to_bytes(decimals=self.decimals)

    def dump(self, file='', compress=False):
        """
        Dumps content of the generator to memory or file: text or gzipped
        :param file: dumps to file if specified
        :param compress: gzip dumped content of a generator
        :return:
        """

        if file:  # dumping to file
            try:
                with open(file, 'wb') as f:
                    if compress:
                        f.write(gzip.compress(b''.join(self.generate(opt=self.opt))))
                    else:
                        for point in self.generate(opt=self.opt):
                            f.write(point)
            except IOError as err:
                logging.error('Error dumping to <%s>: %s', (file, err))

        elif not file:  # in-memory dumping
            if compress:
                points = gzip.compress(b''.join(self.generate(opt=self.opt)))
            else:
                points = b''.join(self.generate(opt=self.opt))
            return points

    def __iter__(self):
        return iter(self.generate(opt=self.opt))




if __name__ == '__main__':

    logging.basicConfig(format='%(levelname)-8s [%(asctime)s]  %(message)s',
                        level=logging.INFO)

    measurement1 = Measurement(name='TestSeries1',
                              fields={'X': -100, 'Y': 720.0, 'T':  30.0},
                              timestamp='2015-12-30 10:36:43.567')
    print('Measurement with datetime string %Y-%m-%d %H:%M:%S.%f as a timestamp: ', measurement1)

    measurement2 = Measurement(name='TestSeries2',
                              fields={'X': -200, 'Y': 700.0, 'T':  25.0},
                              timestamp=12583365412)
    print('Measurement with epoch as a timestamp: ', measurement2)

    measurement3 = Measurement(name='TestSeries3',
                              fields={'X': -20, 'Y': 70.0, 'T':  15.0},
                              timestamp='2015-12-30 10:34:43')
    print('Measurement with datetime string %Y-%m-%d %H:%M:%S as a timestamp: ', measurement3)

    print('Testing <to_bytes>: ', measurement1.to_bytes(decimals=5))
    print('Testing <to_string>: ', measurement1.to_string(decimals=10))
    print('Testing __repr__:', measurement1)

    dummy = DummyPoints(name='TestSeries', npoints=5, decimals=1, delta_seconds=100)
    in_memory_not_compressed = dummy.dump()
    print('Not compressed: ', in_memory_not_compressed)
    in_memory_compressed = dummy.dump(compress=True)
    print('Compressed: ', in_memory_compressed)

    dummy.dump('dump.txt')
    dummy.dump('dump.gz', compress=True)


    dummy2 = DummyPoints(name='TestSeries', npoints=5, decimals=1, delta_seconds=1, opt='single_series')
    for i in range(0, 5):
        print(dummy2.dump())

    container = Container(measurement2)
    print(container.points)
    container.append(measurement1)
    container.append(measurement2)
    container.append(measurement3)
    container.append(measurement1, measurement2, measurement3)
    container.append(*(measurement1, measurement2, measurement3))

    print(container.dump())

    for m in container:
        print(m.to_string())

    print(container)














