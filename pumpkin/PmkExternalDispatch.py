__author__ = 'reggie'



import json
import time
import Queue
import zmq
import copy
import networkx as nx



from networkx.readwrite import json_graph


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
        self.gdisp = ZMQPacketDispatch(self.context, self.context.zmq_context)
        #self.gdisp = ZMQPacketVentilate(self.context, self.context.zmq_context)
        pass

    def getProtoFromEP(self, ep):
        ep.split("://")
        return ep[0]

    def sendPACK(self, pkt):
        ep = pkt[0]["last_contact"]
        if ep in self.dispatchers.keys():
            disp = self.dispatchers[ep]
            disp.dispatch(json.dumps(pkt))
        else:
            disp = self.gdisp
            if not disp == None:
                self.dispatchers[ep] = disp
                disp.connect(ep)
                disp.dispatch(json.dumps(pkt))
                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

            else:
                log.error("No dispatchers found for: "+ep)

            pass

    def run(self):
        graph = self.context.getProcGraph()
        tx = self.context.getTx()
        while True:
            state, otype, pkt = tx.get(True)
            #log.debug("Tx message state: "+ state+" otype: "+otype+" data: "+str(pkt))
            otag = otype+":"+state
            ntag = None
            if pkt[1]:
                g = json_graph.loads(pkt[1])

                if otag in g:
                    d = g[otag]

                    if d:
                        ntag = d.keys()[0]





            while 1:
                routes = graph.getRoutes(otag)
                log.debug("Found routes: "+json.dumps(routes))
                if routes:
                    break
                else:
                    #log.debug("No route found for: "+otag)
                    time.sleep(5)

            for r in routes:
                #log.debug("Route: "+str(r))
                dcpkt = copy.copy(pkt)
                rtag = r["otype"]+":"+r["ostate"]
                if (ntag and ntag == rtag) or not ntag:
                    #TODO make it more flexible not bound to zmq
                    pep = self.context.getProcGraph().getPriorityEndpoint(r)
                    eep = self.context.getProcGraph().getExternalEndpoints(r)
                    oep = self.context.getOurEndpoint(self.getProtoFromEP(pep["ep"]))
                    dcpkt[0]["last_contact"] = oep[0]

                    if pep:
                        entry = pep
                        ep = pep["ep"]
                        #log.debug("Route found for function "+r["name"]+": "+pep["ep"])
                        next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : pep["ep"] }
                        dcpkt.append(next_hop)
                        #pkt.remove( pkt[len(pkt)-1] )
                        #log.debug(json.dumps(pkt))
                        if ep in self.dispatchers.keys():
                            disp = self.dispatchers[ep]
                            disp.dispatch(json.dumps(dcpkt))
                            #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                        else:
                            disp = None
                            if entry["mode"] == "zmq.PULL":
                                #disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                                #disp = ZMQPacketDispatch(self.context)
                                disp = self.gdisp

                            if not disp == None:
                                self.dispatchers[ep] = disp
                                disp.connect(ep)
                                disp.dispatch(json.dumps(dcpkt))
                                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                            else:
                                log.error("No dispatchers found for: "+ep)

                #if r["endpoints"][0]:
                #    entry = r["endpoints"][0]
                #    ep = r["endpoints"][0]["ep"]
                #    log.debug(r["endpoints"][0]["ep"])
                #    next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : r["endpoints"][0]["ep"] }
                #    dcpkt.append(next_hop)
                #    #pkt.remove( pkt[len(pkt)-1] )
                #    #log.debug(json.dumps(pkt))
                #    if ep in self.dispatchers.keys():
                #        disp = self.dispatchers[ep]
                #        disp.dispatch(json.dumps(dcpkt))
                #    else:
                #        disp = None
                #        if entry["mode"] == "zmq.PULL":
                #            disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                #            #disp = ZMQPacketDispatch(self.context)
                #
                #        if not disp == None:
                #            self.dispatchers[ep] = disp
                #            disp.connect(ep)
                #            disp.dispatch(json.dumps(dcpkt))
                #        else:
                #            log.error("No dispatchers found for: "+ep)




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
        log.debug("Created ZMQPacketDispatch")
        if (zmqcontext == None):
            log.debug("Creating zmq context")
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

    def connect(self, connect_to):
        self.soc = self.zmq_cntx.socket(zmq.PUSH)
        self.soc.setsockopt(zmq.HWM, 100)
        #self.soc.setsockopt(zmq.SWAP, 2048*2**10)
        log.debug("ZMQ connecting to :"+str(connect_to))
        self.soc.connect(connect_to)

    def dispatch(self, pkt):

        try:
            self.soc.send(pkt)
        except zmq.ZMQError as e:
            log.error(str(e))

    def close(self):
        self.soc.close()

class ZMQPacketVentilate(Dispatch):

    def __init__(self, context, zmqcontext=None):
        Dispatch.__init__(self)
        self.context = context
        self.soc = None
        log.debug("Created ZMQPacketVentilate")
        if (zmqcontext == None):
            log.debug("Creating zmq context")
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

        self.sender = self.zmq_cntx.socket(zmq.PUSH)
        self.sender.bind("ipc://127.0.0.1:7777")


    def connect(self, connect_to):
        self.soc = self.zmq_cntx.socket(zmq.PUSH)
        log.debug("ZMQ connecting to :"+str(connect_to))
        self.soc.connect(connect_to)
        self.soc.send("REVERSE::ipc://127.0.0.1:7777")

    def dispatch(self, pkt):

        try:
            time.sleep(3)
            log.debug("Sending")
            self.sender.send(pkt)
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
