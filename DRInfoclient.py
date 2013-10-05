__author__ = 'reggie'

import threading
import socket
import time

from socket import *
from threading import *


from DRShared import *


class BroadcastListener(Thread):

    def __init__(self, context, port):
        Thread.__init__(self)
        self.__stop = Event()
        self.__port = port
        self.__context = context
        pass

    def run(self):
        log.info("Starting broadcast listener on port "+str(self.__port))
        sok = socket(AF_INET, SOCK_DGRAM)
        sok.bind(('', self.__port))
        sok.settimeout(5)
        while 1:
            try:
                data, wherefrom = sok.recvfrom(1500, 0)
            except(timeout):
                log.debug("Timeout")
                if self.stopped():
                    log.debug("Exiting thread")
                    break
                else:
                    continue
            log.debug("Broadcast received from: "+repr(wherefrom))
            log.debug("Broadcast data: "+data)

    def stop(self):
        self.__stop.set()

    def stopped(self):
        return self.__stop.isSet()


class Broadcaster(Thread):
    def __init__(self, context, port, rate):
        Thread.__init__(self)
        self.__stop = Event()
        self.__port = port
        self.__rate = rate
        self.__context = context
        pass

    def run(self):
        log.info("Shouting presence to port "+str(self.__port)+" at rate "+str(self.__rate))
        sok = socket(AF_INET, SOCK_DGRAM)
        sok.bind(('', 0))
        sok.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)
        data = self.__context.getUuid() + '\n'
        while 1:
            sok.sendto(data, ('<broadcast>', UDP_BROADCAST_PORT))
            time.sleep(self.__rate)
            if self.stopped():
                break
            else:
                continue

    def stop(self):
        self.__stop.set()

    def stopped(self):
        return self.__stop.isSet()



