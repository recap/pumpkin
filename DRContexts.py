__author__ = 'reggie'

class MainContext(object):
    def __init__(self,uuid, peer=None):
        self.__uuid = uuid
        self.__peer = peer
        self.__args = None
        self.__supernodes = []
        self.__threads = []
        pass

    def setArgs(self, args):
        self.__args = args

    def getUuid(self):
        return self.__uuid

    def getPeer(self):
        return self.__peer

    def isSupernode(self):
        return self.__args.supernode

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








