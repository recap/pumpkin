__author__ = 'reggie'

class MainContext(object):
    def __init__(self,uuid, peer=None):
        self.__uuid = uuid
        self.__peer = peer
        pass

    def getUuid(self):
        return self.__uuid

    def getPeer(self):
        return self.__peer




