__author__ = 'reggie'

import inspect

from DRShared import *

plugins = []
hplugins = {}
iplugins = {}


class PluginType(type):
    def __init__(cls, name, bases, attrs):
        super(PluginType, cls).__init__(name, bases, attrs)

        #print(cls, name, cls.__module__)
        if name != "PluginBase":
            plugins.append(cls)
            #log.debug("Adding: "+name)
            hplugins[name] = cls


class PluginBase(object):
    __metaclass__ = PluginType

    def __init__(self, context, poi="Unset"):
        self.context = context
        self.poi = poi
        self.conf = None


        pass

    def getpoi(self):
        return self.poi

    def setconf(self, jconf):
        self.conf = jconf
        pass

    def getparameters(self):
        print self.getname()
        for p in self.conf["parameters"]:

            sret =  '"itype" : "'+p["type"]+'", "istate" : "'+p["state"]+'"'
            print sret

        return sret

    def getreturn(self):
        for p in self.conf["return"]:
            sret =  '"otype" : "'+p["type"]+'", "ostate" : "'+p["state"]+'"'

        return sret

    def getname(self):
        return self.__class__.__name__


    def on_load(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_load but not implimented.")
        pass


    def on_unload(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_unload but not implimented.")
        pass