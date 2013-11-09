__author__ = 'reggie'

import json

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

        pass


    def setExecContext(self, cntx):
        self.__exec_contex = cntx
        pass
    def getExecContext(self):
        return self.__exec_contex

    def setLocalIP(self,ip):
        self.__ip = ip
        #self.endpoints.append("tcp://"+str(ip)+":"+str(ZMQ_ENDPOINT_PORT))
        self.endpoints.append("ipc://"+self.getUuid())
        pass

    def getLocalIP(self):
        return self.__ip

    def getProcGraph(self):
        return self.proc_graph

    def funcExists(self, func_name):
        if func_name in self.registry.keys():
            return True

        return False

    def setAttributes(self, attributes):
        self.__attrs = attributes

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








