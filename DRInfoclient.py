__author__ = 'reggie'

import threading
import socket


from socket import *
from threading import *


from DRShared import *


class BroadcastListener(object):

    def __init__(self, port):
        self.__port = port
        pass

    def Listen(self):
        sok = socket(AF_INET, SOCK_DGRAM)
        sok.bind(('', self.__port))
        while 1:
            data, wherefrom = sok.recvfrom(1500, 0)
            log.debug("Broadcast received from: "+repr(wherefrom))
            log.debug("Broadcast data: "+data)

    def start(self):
        self.__thread = Thread(target=self.Listen())
        self.__thread.start()

