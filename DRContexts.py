__author__ = 'reggie'

import json

from DRPackets import *

class MainContext(object):
    def __init__(self,uuid, peer=None):
        self.__uuid = uuid
        self.__peer = peer
        self.__args = None
        self.__supernodes = []
        self.__threads = []
        self.rx = rx()
        self.tx = tx()
        self.registry = {}
        pass

    def funcExists(self, func_name):
        if func_name in self.registry.keys():
            return True

        return False


    def updateRegistry(self, entry):
        e = entry
        a = []
        if e["name"] in self.registry.keys():
            log.info("Updating peer: "+e["name"])
            d = self.registry[e["name"]]
            epb = False
            for ep in d["zmq_endpoint"]:
                if ep == e["zmq_endpoint"][0]:
                    epb = True
                    break
            if epb == False:
                d["zmq_endpoint"].append(e["zmq_endpoint"][0])
        else:
            log.info("Discovered new peer: "+e["name"]+" at "+e["zmq_endpoint"][0])
            self.registry[e["name"]] = e

    def dumpRegistry(self):
        d = json.dumps(self.registry)
        return d

    def printRegistry(self):
        for x in self.registry.keys():
            e = self.registry[x]
            log.info("Name: " + e["name"])
            for p in e["zmq_endpoint"]:
                log.info("Endpoint: "+p)
            log.info("Itype: " + e["itype"])
            log.info("Istate: "+e["istate"])
            log.info("Otype: " + e["itype"])
            log.info("Ostate: "+e["istate"])


    def setArgs(self, args):
        self.__args = args

    def getUuid(self):
        return self.__uuid

    def getPeer(self):
        return self.__peer

    def isSupernode(self):
        return self.__args.supernode

    def isWithNoPlugins(self):
        return self.__args.noplugins

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








