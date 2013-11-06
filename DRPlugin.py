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

    def hasInputs(self):
        if len(self.conf["parameters"]) > 0:
            return True

        return False

    def getparameters(self):
        if len(self.conf["parameters"]) > 0:
            for p in self.conf["parameters"]:
                sret =  '"itype" : "'+p["type"]+'", "istate" : "'+p["state"]+'"'
            return sret
        return ' "itype" : "NONE", "istate" : "NONE" '

    def getreturn(self):
        if len(self.conf["return"]) > 0:
            for p in self.conf["return"]:
                sret =  '"otype" : "'+p["type"]+'", "ostate" : "'+p["state"]+'"'
            return sret
        return ' "otype" : "NONE", "ostate" : "NONE" '

    def dispatch(self, msg, state):
        #log.debug(self.conf)
        otype = self.conf["return"][0]["type"]
        self.context.getTx().put((state,otype,msg))
        pass

    def getname(self):
        return self.__class__.__name__


    def on_load(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_load but not implimented.")
        pass


    def on_unload(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_unload but not implimented.")
        pass