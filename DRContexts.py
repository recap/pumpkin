__author__ = 'reggie'

class MainContext(object):
    def __init__(self,uuid):
        self.__uuid = uuid
        pass

    def getUuid(self):
        return self.__uuid
