__author__ = 'reggie'

import collections
import pyinotify
import os
import thread
import zmq
#try:
#    import cPickle as pickle
#except:
#    import pickle

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


class ZMQPacketMonitor(SThread):
    def __init__(self, context, zmqcontext, bind_to):
        SThread.__init__ (self)
        self.context = context
        self.bind_to = bind_to
        if (zmqcontext == None):
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext
        self.rx = self.context.getRx()

    def run(self):
        #context = zmq.Context()
        soc = self.zmq_cntx.socket(zmq.REP)
        soc.bind(self.bind_to)
        #soc.setsockopt(zmq.SUBSCRIBE,self.topic)
        #soc.setsockopt(zmq.RCVTIMEO, 10000)


        while True:
            try:
                msg = soc.recv()
                self.rx.put(msg)
                #log.debug("Message: "+str(msg))
            except zmq.ZMQError as e:
                if self.stopped():
                    log.debug("Exiting thread "+  self.__class__.__name__)
                    soc.close()
                    #zmq_cntx.destroy()
                    #zmq_cntx.term()
                    break
                else:
                    continue

        pass



class ZMQPacketDispatch(SThread):
    def __init__(self, context, zmqcontext,connect_to):
        SThread.__init__(self)
        self.context = context
        self.connect_to = connect_to
        if (zmqcontext == None):
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext
        self.tx = self.context.getTx()

    def run(self):
        #context = zmq.Context()
        soc = self.zmq_cntx.socket(zmq.REQ)
        soc.connect(self.connect_to)
        #soc.setsockopt(zmq.RCVTIMEO, 10000)

        while True:
            try:
                pkt = self.tx.get(True)
                #log.debug("Sending PKT: " + str(pkt))
                soc.send(pkt)
            except zmq.ZMQError as e:
                log.error(str(e))
                if self.stopped():
                    log.debug("Exiting thread "+  self.__class__.__name__)
                    soc.close()
                    #zmq_cntx.destroy()
                    #zmq_cntx.term()
                    break
                else:
                    continue
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
        #mask = pyinotify.IN_CREATE  # watched events
        mask = pyinotify.IN_CLOSE_WRITE  # watched events

        class PTmp(ProcessEvent):
            def __init__(self,context):
                self.context = context
                pass

            def process_IN_CLOSE_WRITE(self, event):
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





