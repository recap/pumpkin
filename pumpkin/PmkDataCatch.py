__author__ = 'reggie'

import json
import pyinotify, os
import PmkShared
import PmkSeed
import time

from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent

from PmkShared import *

class PacketFileMonitor(SThread):

    def __init__(self, context, dir, ext="pkt"):
        SThread.__init__(self)
        self.context = context
        self.dir = dir
        self.ext = ext
        PmkShared._ensure_dir(dir)
        pass

    def run(self):
        #print dir(pyinotify.EventsCodes)

        wm = WatchManager()

        #mask = pyinotify.IN_DELETE | pyinotify.IN_CREATE  # watched events
        #mask = pyinotify.IN_CREATE  # watched events
        #mask = pyinotify.IN_CLOSE_WRITE|pyinotify.ALL_EVENTS  # watched events
        mask = pyinotify.IN_CLOSE_WRITE|pyinotify.IN_MOVE_SELF|pyinotify.IN_MOVED_TO

        class PTmp(ProcessEvent):
            def __init__(self,context):
                self.context = context
                self.tx = self.context.getTx()
                pass

            def load_pkt(self, pktf):
                if( pktf[-3:] == "pkt"):
                    pktd =  open (pktf, "r").read()
                    pktdj = json.loads(pktd)
                    pkt_len = len(pktdj)

                    stag_p = pktdj[pkt_len - 1]["stag"].split(":")


                    if len(stag_p) < 3:
                        group = "public"
                        type = stag_p[0]
                        tag = stag_p[1]
                    else:
                        group = stag_p[0]
                        type = stag_p[1]
                        tag = stag_p[2]

                    self.context.getTx().put((group, tag,type,pktdj))

            def process_IN_MOVE_SELF(self, event):
                pktf = os.path.join(event.path, event.name)
                logging.info("MOVE SELF "+pktf)
                try:
                    self.load_pkt(pktf)
                except Exception as e:
                    logging.error("Loading paket: "+pktf)

            def process_IN_MOVED_TO(self, event):
                pktf = os.path.join(event.path, event.name)
                logging.info("MOVE TO "+pktf)
                try:
                    self.load_pkt(pktf)
                except Exception as e:
                    logging.error("Loading paket: "+pktf)

            def process_IN_CLOSE_WRITE(self, event):
                try:
                    pktf = os.path.join(event.path, event.name)
                    logging.info("INOTIFY CLOSE_WRITE 2: " + str(pktf))
                    self.load_pkt(pktf)

                except Exception as e:
                    logging.error("Loading paket: "+pktf)





            def process_IN_DELETE(self, event):
                print logging.debug("Remove: %s" %  os.path.join(event.path, event.name))


        class Seedmp(ProcessEvent):
            def __init__(self,context):
                self.context = context
                self.tx = self.context.getTx()
                pass

            def load_seed(self, seed_fp):
                if( seed_fp[-2:] == "py"):
                    context = self.context
                    seed_name = context.load_seed(seed_fp)
                    klass = PmkSeed.iplugins[seed_name]
                    js = klass.getConfEntry()
                    self.context.getProcGraph().updateRegistry(json.loads(js), loc="locallocal")


            def process_IN_MOVED_TO(self, event):
                try:
                    seed_fp = os.path.join(event.path, event.name)
                    self.load_seed(seed_fp)
                except Exception as e:
                    logging.error("Loading paket: "+seed_fp)
                    pass

            def process_IN_CLOSE_WRITE(self, event):
                try:
                    seed_fp = os.path.join(event.path, event.name)
                    self.load_seed(seed_fp)
                except Exception as e:
                    logging.error("Loading paket: "+seed_fp)
                    pass

            def process_IN_DELETE(self, event):
                print logging.debug("Remove: %s" %  os.path.join(event.path, event.name))

        if self.ext == "pkt":
            notifier = Notifier(wm, PTmp(self.context))
            wdd = wm.add_watch(self.dir, mask, rec=True)

        if self.ext == "py":
            logging.debug("Adding watch on: "+self.dir)
            notifier = Notifier(wm, Seedmp(self.context))
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
