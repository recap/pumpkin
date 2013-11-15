__author__ = 'reggie'

import json
import Queue
import zmq

import PmkSeed

from PmkShared import *
from Queue import *

class rx(Queue):
    def __init__(self):
        Queue.__init__(self)
        pass

class InternalDispatch(SThread):
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        pass

    def run(self):
        rx = self.context.getRx()
        while 1:
            pkts = rx.get(True)
            log.debug("Packet received: \n"+pkts)
            pkt = json.loads(pkts)
            l = len(pkt)
            func = pkt[l-1]["func"]
            data = pkt[l-2]["data"]

            if func in PmkSeed.iplugins.keys():
                klass = PmkSeed.iplugins[func]
                rt = klass.run(pkt, data)




class Injector(SThread):
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context

    def run(self):
        for x in PmkSeed.iplugins.keys():
            klass = PmkSeed.iplugins[x]
            if not klass.hasInputs():
                #klass.run(klass.__rawpacket())
                klass.rawrun()

            if self.stopped():
                log.debug("Exiting thread "+self.__class__.__name__)
                break
            else:
                continue



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
        soc = self.zmq_cntx.socket(zmq.PULL)
        soc.bind(self.bind_to)
        #soc.setsockopt(zmq.SUBSCRIBE,self.topic)
        #soc.setsockopt(zmq.RCVTIMEO, 10000)


        while True:
            try:
                msg = soc.recv()
                self.rx.put(msg)
                log.debug("Message: "+str(msg))
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



#class InternalDispatch2(Thread):
#    def __init__(self, context):
#        Thread.__init__(self)
#        self.context = context
#        pass
#
#    def run(self):
#
#        rx = self.context.getRx()
#        tx = self.context.getTx()
#        while 1:
#            #fname = rx.get(True)
#            #fh = open(fname, "r")
#            #pkt = fh.read()
#            pkt = rx.get(True)
#            m = re.search('##START-CONF(.+?)##END-CONF(.*)', pkt, re.S)
#            if m:
#                pkt_header = m.group(1)
#                pkt_data = m.group(2)
#                d = json.loads(pkt_header)
#                for fc in d["invoke"]:
#                    state = fc["state"]
#                    if not ((int(state) & DRPackets.READY_STATE) == 1):
#                        func = fc["func"]
#                        log.debug("Trying invoking local function: "+str(func))
#                        if func in DRPlugin.hplugins:
#                            klass = DRPlugin.hplugins[func](self.context)
#                            #klass.on_load()
#                            rt = klass.run(pkt_data)
#                            pkt_data = rt
#                            #xf = klass()
#                            log.debug("RESULT: "+str(rt))
#                            fc["state"] = DRPackets.READY_STATE
#                            opkt = "##START-CONF" + json.dumps(d) + "##END-CONF\n"+str(rt)
#                            log.debug("Out PKT: "+ str(opkt))
#                            #tx.put(opkt)
#
#                            #foutname = "./tx/"+d["container-id"]+d["box-id"]+".pkt"
#                            #fout = open(foutname, "w")
#                            #fout.write(strg)
#                            #fout.flush()
#                            #fout.close()
#                            #log.debug("HERE 1")
#                            #tx.put(d,True)
#                            #log.debug("HERE 2")
#                            #break
#
#                            #log.debug("Return result: "+str(strg))
#                        else:
#                            log.debug("No local function "+func+" found")
#
#
#                    else:
#                        log.debug("Ready moving on")
#
#
#
#                #log.debug("Packet dispatch: "+str(pkt_header))




