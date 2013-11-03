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



class InternalDispatch(Thread):
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
                            klass.on_load()
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



class ExternalDispatch(Thread):
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


