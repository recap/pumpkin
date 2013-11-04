__author__ = 'reggie'

import logging
import threading

from threading import *

LOG_LEVEL = logging.NOTSET
UDP_BROADCAST_PORT = 7700
TFTP_FILE_SERVER_PORT = 7800
#Broadcast presence in intervals
UDP_BROADCAST_RATE = 15
RZV_SERVER_PORT = 7101

HTTP_TCP_IP = '127.0.0.1'
HTTP_TCP_PORT = 8080
HTTP_BUFFER_SIZE = 1024

ZMQ_ENDPOINTS = ["tcp://127.0.0.1:7900"]

SUPERNODES = ["flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvalight.net"]



# Initialize the logger.
#RR#logging.basicConfig()
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

log = logging.getLogger('datariver')


class SThread(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.__stop = Event()

    def stop(self):
        self.__stop.set()

    def stopped(self):
        return self.__stop.isSet()