__author__ = 'reggie'

import logging
import threading
import os

from threading import *

LOG_LEVEL = logging.NOTSET
UDP_BROADCAST_PORT = 7700
TFTP_FILE_SERVER_PORT = 7800
#Broadcast presence in intervals
UDP_BROADCAST_RATE = 15
RZV_SERVER_PORT = 7101

HTTP_TCP_IP = ''
HTTP_TCP_PORT = 7811
HTTP_BUFFER_SIZE = 1024

ZMQ_ENDPOINT_PORT = 7900
ZMQ_PUB_PORT = 7901

#SUPERNODES = ["127.0.0.1", "flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvalight.net"]
#SUPERNODES = [ "127.0.0.1", "elab.lab.uvalight.net", "flightcees.lab.uvalight.net"]
SUPERNODES = [ "127.0.0.1"]




# Initialize the logger.
#logging.basicConfig()
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
log = logging.getLogger('pumpkin')
#log.setLevel(logging.DEBUG)
log.setLevel(logging.INFO)

def _ensure_dir(f):
        if not f[len(f)-1] == "/":
                f = f +"/"
        d = os.path.dirname(f)
        if not os.path.exists(d):
            log.debug(d + " does not exist, creating...")
            os.makedirs(d)
        pass


class SThread(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.__stop = Event()

    def stop(self):
        self.__stop.set()

    def stopped(self):
        return self.__stop.isSet()