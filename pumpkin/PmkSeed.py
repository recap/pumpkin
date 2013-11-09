__author__ = 'reggie'

import inspect
import uuid
import copy
from PmkShared import *

plugins = []
hplugins = {}
iplugins = {}

PKT_NEWBOX = 01
PKT_OLDBOX = 02

class SeedType(type):
    def __init__(cls, name, bases, attrs):
        super(SeedType, cls).__init__(name, bases, attrs)

        #print(cls, name, cls.__module__)
        if name != "Seed":
            plugins.append(cls)
            #log.debug("Adding: "+name)
            hplugins[name] = cls


class Seed(object):
    __metaclass__ = SeedType



    def __init__(self, context, poi="Unset"):
        self.context = context
        self.poi = poi
        self.conf = None


        pass

    def __rawpacket(self):
        pkt = []
        ship_id = self.context.getExecContext()
        cont_id =  str(uuid.uuid4())[:8]
        pkt.append({"ship" : ship_id, "container" : cont_id, "box" : '0', "e" : "E"})
        pkt.append( {"stag" : "RAW", "exstate" : "0001"} )
        return pkt

    def rawrun(self):
        self.run(self.__rawpacket())
        pass

    def run(self, pkt, *args):
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

    def getConfEntry(self):
        js = '{ "name" : "'+self.getname()+'", \
       "zmq_endpoint" : [ {"ep" : "'+self.context.endpoints[0]+'", "cuid" : "'+self.context.getUuid()+'"} ],' \
       ''+self.getparameters()+',' \
       ''+self.getreturn()+'}'
        return js


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

    def dispatch(self, pkt, msg, state, boxing = PKT_OLDBOX):

        if boxing == PKT_NEWBOX:
            lpkt = copy.deepcopy(pkt)
            header = lpkt[0]
            box = int(header["box"])
            box = box + 1
            header["box"] = str(box)
            pkt[0]["box"] = str(box)
        else:
            lpkt = copy.copy(pkt)

        otype = self.conf["return"][0]["type"]
        stag = otype + ":"  + state
        pkt_e = {}
        pkt_e["stag"] = stag
        pkt_e["func"] = self.__class__.__name__
        pkt_e["exstate"] = "0001"
        pkt_e["data"] = msg
        lpkt.append(pkt_e)
        self.context.getTx().put((state,otype,lpkt))
        pass

    def getname(self):
        return self.__class__.__name__


    def on_load(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_load but not implimented.")
        pass


    def on_unload(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_unload but not implimented.")
        pass