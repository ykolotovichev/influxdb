__author__ = 'Yury A. Kolotovichev'
import logging

class InfluxdbAPICodeMismatchError(Exception):
    def __init__(self, content, code_received, codes_expected):
        message = "Code %s. Expected %s. %s" % (code_received, str(codes_expected), content)

        super(InfluxdbAPICodeMismatchError, self).__init__(message)

        self.content = content
        self.code_received = code_received

        logging.error('Code %d. Expected %s. InfluxdbClientError: %s' % (self.code_received, codes_expected,
                                                                         self.content))

class InfluxdbAPIRequestError(Exception):
    def __init__(self, message=''):

        super(InfluxdbAPIRequestError, self).__init__(message)
        logging.error('Error sending HTTP request to Influxdb server')




if __name__ == '__main__':

    logging.basicConfig(format='%(levelname)-8s [%(asctime)s]  %(message)s',
                        level=logging.DEBUG)

    raise InfluxdbAPICodeMismatchError('Timeout', 500, (200, 400))

    #raise InfluxdbAPIRequestError('Test Message')

