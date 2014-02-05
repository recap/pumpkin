__author__ = 'reggie'

import logging
import threading
import os
import socket

from socket import *

from threading import *

LOG_LEVEL = logging.NOTSET
UDP_BROADCAST_PORT = 7700
TFTP_FILE_SERVER_PORT = 7800
#Broadcast presence in intervals
UDP_BROADCAST_RATE = 15
RZV_SERVER_PORT = 7101

HTTP_TCP_IP = ''
#HTTP_TCP_PORT = _get_nextport(7811)
# 7811
HTTP_BUFFER_SIZE = 1024

ZMQ_ENDPOINT_PORT = 7900
ZMQ_PUB_PORT = 7901

#SUPERNODES = ["127.0.0.1", "flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvalight.net"]
#SUPERNODES = [ "elab.lab.uvalight.net"]
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
            os.makedirs(d,mode=0775)
            os.chmod(d,0775)
        pass

def _get_nextport(port, prot="TCP"):
         err = True
         lport = int(port)
         while err == True:
             err = False
             if prot == "UDP":
                 sock = socket(AF_INET, SOCK_DGRAM)
             else:
                 sock = socket(AF_INET, SOCK_STREAM)
             try:
                sock.bind(('0.0.0.0', lport))

             except Exception as e:
                 sock.close()
                 err = True
                 log.warn("Port already in use: "+str(lport))
                 lport += 1

                 continue


         sock.close()
         return lport


class SThread(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.__stop = Event()

    def stop(self):
        self.__stop.set()

    def stopped(self):
        return self.__stop.isSet()



HTTP_TCP_PORT = _get_nextport(7811)
