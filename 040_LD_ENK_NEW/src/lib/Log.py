from datetime import datetime
import logging

def Log_Info(file_name, status):
    logging.basicConfig(filename=file_name, level=logging.DEBUG)
    now = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.now())
    print(now, status)
    logging.info('%s %s', now, status)

def Log_Error(file_name, status):
    logging.basicConfig(filename=file_name, level=logging.DEBUG)
    now = '{0:%Y-%m-%d %H:%M:%S}'.format(datetime.now())
    print(now, status)
    logging.error('%s %s', now, status)