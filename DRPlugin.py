__author__ = 'reggie'

import inspect

from DRShared import *

plugins = []
hplugins = {}


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
        self.istate = {}
        self.ostate = {}

        pass

    def getpoi(self):
        return self.poi


    def getname(self):
        return self.__class__.__name__


    def on_load(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_load but not implimented.")
        pass


    def on_unload(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_unload but not implimented.")
        pass