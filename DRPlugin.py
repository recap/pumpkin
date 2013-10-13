__author__ = 'reggie'

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
        pass

    def getpoi(self):
        return self.poi

    def on_load(self):
        pass

    def run(self, *args):
        pass

    def on_unload(self):
        pass