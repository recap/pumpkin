__author__ = 'reggie'

import os
import re
import json
import tftpy
import DRPlugin
import DRPackets

from threading import *
from Queue import *



from DRShared import *
from DRContexts import *
from DRPackets import *
from DRPlugin import *


class InternalDispatch(SThread):
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        pass

    def run(self):
        rx = self.context.getRx()
        while 1:
            pkt = json.loads(rx.get(True))
            l = len(pkt)
            func = pkt[l-1]["func"]
            data = pkt[l-2]["data"]
            log.debug(data)
            if func in DRPlugin.iplugins.keys():
                klass = DRPlugin.iplugins[func]
                rt = klass.run(pkt, data)
                log.debug("RESULT: "+str(rt))



class InternalDispatch2(Thread):
    def __init__(self, context):
        Thread.__init__(self)
        self.context = context
        pass

    def run(self):

        rx = self.context.getRx()
        tx = self.context.getTx()
        while 1:
            #fname = rx.get(True)
            #fh = open(fname, "r")
            #pkt = fh.read()
            pkt = rx.get(True)
            m = re.search('##START-CONF(.+?)##END-CONF(.*)', pkt, re.S)
            if m:
                pkt_header = m.group(1)
                pkt_data = m.group(2)
                d = json.loads(pkt_header)
                for fc in d["invoke"]:
                    state = fc["state"]
                    if not ((int(state) & DRPackets.READY_STATE) == 1):
                        func = fc["func"]
                        log.debug("Trying invoking local function: "+str(func))
                        if func in DRPlugin.hplugins:
                            klass = DRPlugin.hplugins[func](self.context)
                            #klass.on_load()
                            rt = klass.run(pkt_data)
                            pkt_data = rt
                            #xf = klass()
                            log.debug("RESULT: "+str(rt))
                            fc["state"] = DRPackets.READY_STATE
                            opkt = "##START-CONF" + json.dumps(d) + "##END-CONF\n"+str(rt)
                            log.debug("Out PKT: "+ str(opkt))
                            #tx.put(opkt)

                            #foutname = "./tx/"+d["container-id"]+d["box-id"]+".pkt"
                            #fout = open(foutname, "w")
                            #fout.write(strg)
                            #fout.flush()
                            #fout.close()
                            #log.debug("HERE 1")
                            #tx.put(d,True)
                            #log.debug("HERE 2")
                            #break

                            #log.debug("Return result: "+str(strg))
                        else:
                            log.debug("No local function "+func+" found")


                    else:
                        log.debug("Ready moving on")



                #log.debug("Packet dispatch: "+str(pkt_header))



class ExternalDispatch2(Thread):
    def __init__(self, context):
        Thread.__init__(self)
        self.context = context
        pass

    def run(self):
        tx = self.context.getTx()
        log.debug("HERE 3")
        d = tx.get(True)
        log.debug("HERE 4")
        foutname = "./tx/"+d["container-id"]+d["box-id"]+".pkt"
        foutnames = d["container-id"]+d["box-id"]+".pkt"
        for fc in d["invoke"]:
            state = fc["state"]
            if not ((int(state) & DRPackets.READY_STATE) == 1):
                func = fc["func"]
                log.debug("Function "+func)
                peer = self.context.getMePeer().getPeerForFunc(func)
                if not peer == None:
                    comm = peer.getTftpComm()
                    log.debug("Got comm")
                    if not comm == None:
                        log.debug("Comm to "+comm.host+" "+str(comm.port))
                        client = tftpy.TftpClient(str(comm.host), int(comm.port))
                        log.debug("Files: "+foutnames+" "+foutname)
                        tmpf = open(foutname, "r")
                        fstr = tmpf.read()
                        log.debug("File: "+fstr)
                        tmpf.close()
                        client.upload(foutnames.encode('utf-8'),foutname.encode('utf-8'))
                        #client.upload("test.pkt","./tx/test.pkt")
                        break
                else:
                    log.warn("No peer found for function "+func)

class Injector(SThread):
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context




    def run(self):
        for x in DRPlugin.iplugins.keys():
            klass = DRPlugin.iplugins[x]
            if not klass.hasInputs():
                #klass.run(klass.__rawpacket())
                klass.rawrun()

            if self.stopped():
                log.debug("Exiting thread "+self.__class__.__name__)
                break
            else:
                continue


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

                #TODO make it more flexible not bound to zmq
                if r["zmq_endpoint"][0]:
                    ep = r["zmq_endpoint"][0]["ep"]
                    #log.debug(r["zmq_endpoint"][0]["ep"])
                    next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : r["zmq_endpoint"][0]["ep"] }
                    pkt.append(next_hop)
                    #pkt.remove( pkt[len(pkt)-1] )
                    #log.debug(json.dumps(pkt))
                    if ep in self.dispatchers.keys():
                        disp = self.dispatchers[ep]
                        disp.dispatch(json.dumps(pkt))
                    else:
                        disp = ZMQPacketDispatch(self.context)
                        self.dispatchers[ep] = disp
                        disp.connect(ep)
                        disp.dispatch(json.dumps(pkt))




            if self.stopped():
                log.debug("Exiting thread "+self.__class__.__name__)
                break
            else:
                continue



