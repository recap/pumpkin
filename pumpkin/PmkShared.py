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


TIMING_BIT =            0b1
TRACK_BIT =             0b10
ACK_BIT =               0b100
TRACER_BIT =            0b1000


#SUPERNODES = ["127.0.0.1", "flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvalight.net"]
#SUPERNODES = [ "elab.lab.uvalight.net"]
SUPERNODES = [ "127.0.0.1"]




# Initialize the logger.
#logging.basicConfig()
##logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
##log = logging.getLogger('pumpkin')


#logging.setLevel(logging.DEBUG)
##logging.setLevel(logging.INFO)



def initialize_logger(output_dir, console=True):
    logger = logging.getLogger()
    for h in logger.handlers:
        logger.removeHandler(h)

    logger.setLevel(logging.DEBUG)


    if console:
       # create console handler and set level to info
       handler = logging.StreamHandler()
       handler.setLevel(logging.DEBUG)
       formatter = logging.Formatter("%(levelname)s - %(message)s")
       handler.setFormatter(formatter)
       logger.addHandler(handler)

    # create error file handler and set level to error
    #handler = logging.FileHandler(os.path.join(output_dir, "error.log"),"w", encoding=None, delay="true")
    #handler.setLevel(logging.ERROR)
    #formatter = logging.Formatter("%(levelname)s - %(message)s")
    #handler.setFormatter(formatter)
    #logger.addHandler(handler)

    # create debug file handler and set level to debug
    handler = logging.FileHandler(os.path.join(output_dir, "all.log"),"w")
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)





def _ensure_dir(f):
        if not f[len(f)-1] == "/":
                f = f +"/"
        d = os.path.dirname(f)
        if not os.path.exists(d):
            logging.debug(d + " does not exist, creating...")
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
                 logging.warn("Port already in use: "+str(lport))
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
