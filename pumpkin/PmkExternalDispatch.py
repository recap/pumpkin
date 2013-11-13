__author__ = 'reggie'



import json
import time
import Queue
import zmq
import copy

from Queue import *
from PmkShared import *


class tx(Queue):
    def __init__(self):
        Queue.__init__(self)
        pass




class ExternalDispatch(SThread):

    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        self.dispatchers = {}
        pass


    def run(self):
        graph = self.context.getProcGraph()
        tx = self.context.getTx()
        while True:
            state, otype, pkt = tx.get(True)
            #log.debug("Tx message state: "+ state+" otype: "+otype+" data: "+str(pkt))
            otag = otype+":"+state

            while 1:
                routes = graph.getRoutes(otag)
                if routes:
                    break
                else:
                    log.debug("No route found for: "+otag)
                    time.sleep(5)

            for r in routes:
                #log.debug("Route: "+str(r))
                dcpkt = copy.copy(pkt)
                #TODO make it more flexible not bound to zmq
                if r["endpoints"][0]:
                    entry = r["endpoints"][0]
                    ep = r["endpoints"][0]["ep"]
                    #log.debug(r["zmq_endpoint"][0]["ep"])
                    next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : r["endpoints"][0]["ep"] }
                    dcpkt.append(next_hop)
                    #pkt.remove( pkt[len(pkt)-1] )
                    #log.debug(json.dumps(pkt))
                    if ep in self.dispatchers.keys():
                        disp = self.dispatchers[ep]
                        disp.dispatch(json.dumps(dcpkt))
                    else:
                        disp = None
                        if entry["mode"] == "zmq.PULL":
                            disp = ZMQPacketDispatch(self.context)

                        if not disp == None:
                            self.dispatchers[ep] = disp
                            disp.connect(ep)
                            disp.dispatch(json.dumps(dcpkt))
                        else:
                            log.error("No dispatchers found for: "+ep)




            if self.stopped():
                log.debug("Exiting thread "+self.__class__.__name__)
                for ep in self.dispatchers.keys():
                    disp = disp = self.dispatchers[ep]
                    disp.close()
                break
            else:
                continue



class Dispatch(object):
    def connect(self, connect_to):
        pass
    def dispatch(self, pkt):
        pass
    def close(self):
        pass

class ZMQPacketPublish(Dispatch):
    def __init__(self, context, zmqcontext=None):
        Dispatch.__init__(self)
        self.context = context
        self.soc = None
        if (zmqcontext == None):
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

    def connect(self, connect_to):
        self.soc = self.zmq_cntx.socket(zmq.PUB)
        self.soc.bind(connect_to)

    def dispatch(self, pkt):
        try:
            self.soc.send(pkt)
        except zmq.ZMQError as e:
            log.error(str(e))

    def close(self):
        self.soc.close()


class ZMQPacketDispatch(Dispatch):
    def __init__(self, context, zmqcontext=None):
        Dispatch.__init__(self)
        self.context = context
        self.soc = None
        if (zmqcontext == None):
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

    def connect(self, connect_to):
        self.soc = self.zmq_cntx.socket(zmq.PUSH)
        self.soc.connect(connect_to)

    def dispatch(self, pkt):

        try:
            self.soc.send(pkt)
        except zmq.ZMQError as e:
            log.error(str(e))

    def close(self):
        self.soc.close()


#class ExternalDispatch2(Thread):
#    def __init__(self, context):
#        Thread.__init__(self)
#        self.context = context
#        pass
#
#    def run(self):
#        tx = self.context.getTx()
#        log.debug("HERE 3")
#        d = tx.get(True)
#        log.debug("HERE 4")
#        foutname = "./tx/"+d["container-id"]+d["box-id"]+".pkt"
#        foutnames = d["container-id"]+d["box-id"]+".pkt"
#        for fc in d["invoke"]:
#            state = fc["state"]
#            if not ((int(state) & DRPackets.READY_STATE) == 1):
#                func = fc["func"]
#                log.debug("Function "+func)
#                peer = self.context.getMePeer().getPeerForFunc(func)
#                if not peer == None:
#                    comm = peer.getTftpComm()
#                    log.debug("Got comm")
#                    if not comm == None:
#                        log.debug("Comm to "+comm.host+" "+str(comm.port))
#                        client = tftpy.TftpClient(str(comm.host), int(comm.port))
#                        log.debug("Files: "+foutnames+" "+foutname)
#                        tmpf = open(foutname, "r")
#                        fstr = tmpf.read()
#                        log.debug("File: "+fstr)
#                        tmpf.close()
#                        client.upload(foutnames.encode('utf-8'),foutname.encode('utf-8'))
#                        #client.upload("test.pkt","./tx/test.pkt")
#                        break
#                else:
#                    log.warn("No peer found for function "+func)
