__author__ = 'reggie'

import json

from DRPackets import *
from DRProcessGraph import *

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
        self.__ip = "127.0.0.1"
        self.endpoints = []
        self.__reg_update = False
        self.rlock = threading.RLock()
        self.proc_graph = ProcessGraph()


        pass



    def setLocalIP(self,ip):
        self.__ip = ip
        self.endpoints.append("tcp://"+str(ip)+":"+str(ZMQ_ENDPOINT_PORT))
        pass

    def getLocalIP(self):
        return self.__ip

    def getProcGraph(self):
        return self.proc_graph

    def funcExists(self, func_name):
        if func_name in self.registry.keys():
            return True

        return False

    def setArgs(self, args):
        self.__args = args

    def getUuid(self):
        return self.__uuid

    def getPeer(self):
        return self.__peer

    def getTaskDir(self):
        return self.__args.taskdir

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








