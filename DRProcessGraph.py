__author__ = 'reggie'

import json
import networkx as nx
import matplotlib.pyplot as plt

from DRPackets import *

class ProcessGraph(object):
    def __init__(self):
        self.registry = {}
        self.__reg_update = False
        self.rlock = threading.RLock()
        self.graph = nx.DiGraph()
    pass

    def updateRegistry(self, entry):
        e = entry
        a = []
        self.rlock.acquire()
        if e["name"] in self.registry.keys():
            log.info("Updating peer: "+e["name"])
            d = self.registry[e["name"]]
            epb = False
            for ep in d["zmq_endpoint"]:
                if ep["ep"] == e["zmq_endpoint"][0]["ep"]:
                    epb = True
                    break
            if epb == False:
                d["zmq_endpoint"].append(e["zmq_endpoint"][0])
                self.__reg_update = True
        else:
            log.info("Discovered new peer: "+e["name"]+" at "+e["zmq_endpoint"][0]["ep"])
            self.registry[e["name"]] = e
            self.__reg_update = True

        if self.__reg_update ==True:
            self.graph = self.buildGraph()

        self.rlock.release()

    def buildGraph(self):
        G = self.graph
        for xo in self.registry.keys():
            eo = self.registry[xo]


            for isp in eo["istate"].split('|'):
                for osp in eo["ostate"].split('|'):
                    istype = eo["itype"]+":"+isp
                    if istype == "NONE:NONE":
                           istype = "RAW"
                    ostype = eo["otype"]+":"+osp
                    G.add_edge(istype, ostype, function=eo["name"])
        return G

    def showGraph(self):
        nx.draw(self.graph)
        plt.show()
        pass


    def isRegistryModified(self):
        return self.__reg_update

    def ackRegistryUpdate(self):
        self.rlock.acquire()
        self.__reg_update = False
        self.rlock.release()
        pass

    def dumpRegistry(self):
        self.rlock.acquire()
        d = json.dumps(self.registry)
        self.rlock.release()
        return d

    def printRegistry(self):
        for x in self.registry.keys():
            e = self.registry[x]
            log.info("Name: " + e["name"])
            for p in e["zmq_endpoint"]:
                log.info("Endpoint: "+p)
            log.info("Itype: " + e["itype"])
            log.info("Istate: "+e["istate"])
            log.info("Otype: " + e["itype"])
            log.info("Ostate: "+e["istate"])
