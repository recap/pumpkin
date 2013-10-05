__author__ = 'reggie'

import threading
import socket


from socket import *
from threading import *


from DRShared import *


class BroadcastListener(Thread):

    def __init__(self, port):
        Thread.__init__(self)
        self.__port = port
        pass

    def run(self):
        sok = socket(AF_INET, SOCK_DGRAM)
        sok.bind(('', self.__port))
        while 1:
            data, wherefrom = sok.recvfrom(1500, 0)
            log.debug("Broadcast received from: "+repr(wherefrom))
            log.debug("Broadcast data: "+data)

   # def start(self):
        #self.__thread = Thread(target=self.Listen())
        #self.__thread.start()


#class Typewriter(threading.Thread):
#    def __init__(self, your_string):
#        threading.Thread.__init__(self)
#        self.my_string = your_string

#    def run(self):
        #for char in my_string:
        #    libtcod.console_print(0,3,3,char)
        #    time.sleep(50)

