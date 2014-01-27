__author__ = 'reggie'

import json
import pyinotify, os
import PmkShared

from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent

from PmkShared import *

class PacketFileMonitor(SThread):

    def __init__(self, context, dir):
        SThread.__init__(self)
        self.context = context
        self.dir = dir
        PmkShared._ensure_dir(dir)
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
                self.tx = self.context.getTx()
                pass

            def process_IN_CLOSE_WRITE(self, event):
                try:
                    pktf = os.path.join(event.path, event.name)
                    if( pktf[-3:] == "pkt"):
                        pktd =  open (pktf, "r").read()
                        pktdj = json.loads(pktd)
                        pkt_len = len(pktdj)
                        type = pktdj[pkt_len - 1]["stag"].split(":")[0]
                        tag = pktdj[pkt_len - 1]["stag"].split(":")[1]
                        self.context.getTx().put((tag,type,pktdj))
                except Exception as e:
                    log.error("Loading paket: "+pktf)
                    pass


            def process_IN_DELETE(self, event):
                print log.debug("Remove: %s" %  os.path.join(event.path, event.name))

        notifier = Notifier(wm, PTmp(self.context))

        wdd = wm.add_watch(self.dir, mask, rec=True)

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
