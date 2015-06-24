__author__ = 'Yury'

from time import time
from random import random, randint
from datetime import datetime




# line protocol
class Measurement:
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

class Dummy:

    def __init__(self, series,  npoints=1, decimals=3):
        self.series = series
        self.npoints = npoints
        self.decimals = decimals

    def generate(self):
        start_epoch = randint(1e+9, int(time()*1e+9))
        for i in range(0, self.npoints):
            m = Measurement(name=self.series,
                            fields={'X': random() * -720.0, 'Y': random() * 720.0, 'T': random() * 30.0},
                            timestamp=start_epoch + i)

            yield m.to_bytes(decimals=self.decimals)

    def to_bytes(self):
        points = b''.join(self.generate())
        return points


#comment
if __name__ == '__main__':

        measurement = Measurement(name='TestSeries',
                                  fields={'X': -100, 'Y': 720.0, 'T':  30.0},
                                  timestamp='2015-12-30 10:36:43.567')

        print(measurement.to_bytes(decimals=5))


        dummy = Dummy(series='TestSeries', npoints=3, decimals=6)
        print(next(dummy.generate()))
        print(dummy.to_bytes())
