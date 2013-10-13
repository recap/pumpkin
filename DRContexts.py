__author__ = 'reggie'

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
        pass

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








