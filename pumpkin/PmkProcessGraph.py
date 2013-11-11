__author__ = 'reggie'

import json
import networkx as nx
import time
import thread
import threading

from networkx.readwrite import json_graph
from PmkShared import *


class ProcessGraph(object):

    def __init__(self):

        self.registry = {}
        self.__reg_update = False
        self.__display_graph = False
        self.rlock = threading.RLock()
        self.graph = nx.DiGraph()
        self.tagroute = {}


    pass

    def updateRegistry(self, entry):
        e = entry
        a = []
        self.rlock.acquire()
        if e["name"] in self.registry.keys():
            log.info("Updating peer: "+e["name"])
            d = self.registry[e["name"]]
            epb = False
            for ep in d["endpoints"]:
                if ep["ep"] == e["endpoints"][0]["ep"]:
                    epb = True
                    break
            if epb == False:
                d["endpoints"].append(e["endpoints"][0])
                self.__reg_update = True
                self.__display_graph = True
        else:
            log.info("Discovered new peer: "+e["name"]+" at "+e["endpoints"][0]["ep"])
            self.registry[e["name"]] = e
            self.__reg_update = True
            self.__display_graph = True

        if self.__reg_update == True:
            self.graph = self.buildGraph()

        self.rlock.release()

    def displayGraph(self):
        return self.__display_graph

    def resetDisplay(self):
        self.__display_graph = False

    def getRoutes(self, tag):
        log.debug("Finding route for "+ tag)
        if tag in self.tagroute:
            return self.tagroute[tag]

    def buildGraph(self):
        self.tagroute = {}
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
                if istype in self.tagroute.keys():
                    self.tagroute[istype].append(eo)
                else:
                    self.tagroute[istype] = []
                    self.tagroute[istype].append(eo)
        return G

    def showGraph(self):
        import matplotlib.pyplot as plt
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

    def dumpGraph(self):
        d = json_graph.node_link_data(self.graph)
        return json.dumps(d)
        #return str(nx.generate_graphml(self.graph))

    def dumpGraphToFile(self, filename):
        d = json_graph.node_link_data(self.graph)
        json.dump(d, open(filename,'w'))

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
