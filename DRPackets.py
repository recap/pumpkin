__author__ = 'reggie'

import collections
import pyinotify
import os

from Queue import *
from collections import *
from threading import *
from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent

from DRShared import *

READY_STATE = 00000001



class rx(Queue):
    def __init__(self):
        Queue.__init__(self)
        pass

class tx(Queue):
    def __init__(self):
        Queue.__init__(self)
        pass



class PacketFileMonitor(Thread):

    def __init__(self, context):
        Thread.__init__(self)
        self.__context = context
        pass

    def run(self):
        #print dir(pyinotify.EventsCodes)

        wm = WatchManager()

        #mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE  # watched events
        mask = pyinotify.IN_CREATE  # watched events

        class PTmp(ProcessEvent):
            def __init__(self,context):
                self.context = context
                pass

            def process_IN_CREATE(self, event):
                #print "Create: %s" %  os.path.join(event.path, event.name)
                pkt = os.path.join(event.path, event.name)
                rx = self.context.getRx()
                log.debug("Queueing RX packet")
                rx.put(pkt, True)


            def process_IN_DELETE(self, event):
                print "Remove: %s" %  os.path.join(event.path, event.name)

        notifier = Notifier(wm, PTmp(self.__context))

        wdd = wm.add_watch('./rx', mask, rec=True)

        while True:  # loop forever
            try:
                # process the queue of events as explained above
                notifier.process_events()
                if notifier.check_events():
                    # read notified events and enqeue them
                    notifier.read_events()
                # you can do some tasks here...
            except KeyboardInterrupt:
                # destroy the inotify's instance on this interrupt (stop monitoring)
                notifier.stop()
                break

        pass



