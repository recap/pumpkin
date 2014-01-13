__author__ = 'reggie'

import sys
import shelve

from PmkExternalDispatch import *
from PmkInternalDispatch import *

from PmkProcessGraph import *

class MainContext(object):
    def __init__(self,uuid, peer=None):
        self.__uuid = uuid
        self.__peer = peer
        self.__attrs = None
        self.__supernodes = []
        self.__threads = []
        self.rx = rx()
        self.tx = tx()
        self.registry = {}
        self.__ip = "127.0.0.1"
        self.endpoints = []
        self.__reg_update = False
        self.rlock = threading.RLock()
        self.proc_graph = ProcessGraph()
        self.__exec_context = None
        self.zmq_context = None
        self.working_dir = "./.pumpkin/"+self.__uuid
        self.file_dir = None
        self.external_dispatch = None



        pass

    def startPktShelve(self, filename):
        self.pkt_shelve = shelve.open(self.working_dir+"/"+filename)
        pass

    def getPktId(self, pkt):
        id= pkt[0]["ship"]+":"+pkt[0]["container"]+":"+pkt[0]["box"]+":"+pkt[0]["fragment"]
        return id

    def pktReady(self, pkt):
        pkt_id = str(self.getPktId(pkt))
        self.pkt_shelve[pkt_id] = pkt
        pass

    def isPktShelved(self, pkt):
        pkt_id = str(self.getPktId(pkt))
        if pkt_id in self.pkt_shelve:
            return True
        return False

    def setExternalDispatch(self, dispatch):
        self.external_dispatch = dispatch
        pass

    def getExternalDispatch(self):
        return self.external_dispatch

    def setFileDir(self, file_dir):
        self.file_dir = file_dir
        pass

    def getFileDir(self):
        return self.file_dir

    def getFileServerEndPoint(self):
        ep = "tftp://"+self.getLocalIP()+":"+str(TFTP_FILE_SERVER_PORT)
        return ep

    def getWorkingDir(self):
        return self.working_dir

    def setExecContext(self, cntx):
        self.__exec_contex = cntx
        pass
    def getExecContext(self):
        return self.__exec_contex

    def setLocalIP(self,ip):
        self.__ip = ip

        #FIXME dynamic configuration
        #self.endpoints.append( ("ipc://"+self.getUuid(), "zmq.ipc", "zmq.PULL" ) )
        #self.endpoints.append( ("ipc://"+self.getUuid(), "zmq.ipc", "zmq.PUB" ) )
        #self.endpoints.append(("tcp://"+str(ip)+":"+str(ZMQ_ENDPOINT_PORT), "zmq.tcp", "zmq.PULL"))

        pass


    #def getEndpoint(self):
    #    #if self.__attrs.eptype == "zmq.TCP":
    #    #    return "tcp://"+str(self.__ip)+":"+str(ZMQ_ENDPOINT_PORT)
    #    #if self.__attrs.eptype == "zmq.IPC":
    #    #    return "ipc:///tmp/"+self.getUuid()
    #    return "inproc://"+self.getUuid()

    def isZMQEndpoint(self, entry):
        e = str(entry[1])
        if "zmq" in e.lower():
            return True
        return False

    def getEndpoints(self):
        return self.endpoints

    def hasRx(self):
        return self.__attrs.rxdir

    def singleSeed(self):
        return self.__attrs.singleseed

    def getLocalIP(self):
        return self.__ip

    def getProcGraph(self):
        return self.proc_graph

    def funcExists(self, func_name):
        if func_name in self.registry.keys():
            return True

        return False

    def getAttributeValue(self):
        return self.__attrs

    def getOurEndpoint(self, proto):
        tcp_ep = None
        for ep in self.endpoints:
            if str(ep[0]).startswith(proto):
                return ep
            if str(ep[0]).startswith("tcp://"):
                tcp_ep = ep

        log.warning("Found no endpoint matching defaulting to tcp")
        return tcp_ep

    def setEndpoints(self):
        if self.__attrs.eps == "ALL":
            self.__attrs.eps = "tftp://*:*/*;inproc://*;ipc://*;tcp://*:*"
        epl = self.__attrs.eps.split(";")
        for ep in epl:
            prts = ep.split("//")
            prot = prts[0]
            if prot == "inproc:":
                if prts[1] == "*":
                    s = "inproc://"+self.getUuid()
                else:
                    s = ep
                self.endpoints.append( (s, "zmq.INPROC", "zmq.PULL", 1) )
                log.debug("Added endpoint: "+s)

            elif prot == "ipc:":
                if prts[1] == "*":
                    s = "ipc:///tmp/"+self.getUuid()
                else:
                    s = ep
                self.endpoints.append( (s, "zmq.IPC", "zmq.PULL", 2) )
                log.debug("Added endpoint: "+s)

            elif prot == "tcp:":
                addr = prts[1].split(":")
                if addr[0] == "*":
                    addr[0] = self.__ip
                if addr[1] == "*":
                    addr[1] = str(ZMQ_ENDPOINT_PORT)

                s = "tcp://"+addr[0]+":"+addr[1]
                self.endpoints.append( (s, "zmq.TCP", "zmq.PULL", 5) )
                log.debug("Added endpoint: "+s)

            #TODO uncomment once tftp is integrated
            #elif prot == "tftp:":
            #
            #    addrprts = prts[1].split("/")
            #    addr = addrprts[0].split(":")
            #    dir_path = addrprts[1]
            #    if addr[0] == "*":
            #        addr[0] = self.__ip
            #    if addr[1] == "*":
            #        addr[1] = str(TFTP_FILE_SERVER_PORT)
            #    if dir_path == "*":
            #        dir_path = "/tmp/tftproot/"
            #
            #    s = "tftp://"+addr[0]+":"+addr[1]+"/"+dir_path
            #    self.endpoints.append( (s, "tftp.UDP", "tftp.SERVER") )
            #    log.debug("Added endpoint: "+s)
            else:
                log.warning("Unknown endpoint: "+ep)



    def setAttributes(self, attributes):
        self.__attrs = attributes
        if attributes.debug:
            log.setLevel(logging.DEBUG)
        else:
            log.setLevel(logging.INFO)

        self.setEndpoints()

        #epm = attributes.epmode
        #ept = attributes.eptype
        #prot = ept.split('.')[1].lower()
        #if prot == "tcp":
        #    s = prot+"://"+self.__ip+":"+str(ZMQ_ENDPOINT_PORT)
        #if prot == "ipc":
        #    s = prot+":///tmp/"+self.getUuid()
        #if prot == "inproc":
        #    s = prot+"://"+self.getUuid()
        #
        #self.endpoints.append( (s, ept, epm) )

    def getUuid(self):
        return self.__uuid

    def getPeer(self):
        return self.__peer

    def getTaskDir(self):
        return self.__attrs.taskdir

    def hasShell(self):
        return self.__attrs.shell

    def isSupernode(self):
        return self.__attrs.supernode

    def isWithNoPlugins(self):
        return self.__attrs.noplugins

    def setSupernodeList(self, sn):
        self.__supernodes = sn

    def getSupernodeList(self):
        return self.__supernodes

    def addThread(self, th):
        self.__threads.append(th)

    def getThreads(self):
        return self.__threads

    def setMePeer(self, peer):
        self.__peer = peer

    def getMePeer(self):
        return self.__peer

    def getRx(self):
        return self.rx

    def getTx(self):
        return self.tx


    def close(self):
        self.pkt_shelve.close()






