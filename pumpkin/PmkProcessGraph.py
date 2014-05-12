__author__ = 'reggie'

import json
import networkx as nx
import time
import thread
import threading
import copy

from networkx.readwrite import json_graph

from PmkShared import *


class ProcessGraph(object):

    MAX_TTL = 60 #seconds
    INT_TTL = 15 #seconds
    def __init__(self):

        self.registry = {}
        self.external_registry = {}
        self.__reg_update = False
        self.__display_graph = False
        self.rlock = threading.RLock()
        self.graph = nx.DiGraph()
        self.tagroute = {}
        self.ttl = {}

        threading.Timer(self.INT_TTL, self.__update_registry_t).start()

    pass

    def __key(self,name, ep):
        return name+":|:"+ep

    def __reset_ep_ttl(self, name, ep):
        key = self.__key(name, ep)
        self.ttl[key] = self.MAX_TTL
        pass

    def __subtract_ep_ttl(self, name, ep, sub_count):
        key = self.__key(name,ep)
        if key in self.ttl.keys():
            self.ttl[key] -= sub_count
        pass
    def __split_key(self, key):
        spl = key.split(":|:")
        return (spl[0], spl[1])

    def __del_ep(self, key):

        name, ep = self.__split_key(key)
        if name in self.registry.keys():
            e = self.registry[name]
            c = -1
            for i in range(len(e["endpoints"])):
                if e["endpoints"][i]["ep"] == ep:
                    c = i
                    break
            if c > -1:
                log.info("Removed stale entry for seed "+name+" at "+ep)
                del e["endpoints"][c]


    def __update_registry_t(self):
        self.rlock.acquire()
        keys_for_removal = []
        for key in self.ttl.keys():
            self.ttl[key] -= self.INT_TTL
            if self.ttl[key] <= 0:
                self.__del_ep(key)
                keys_for_removal.append(key)


        for rk in keys_for_removal:
            del self.ttl[rk]

        self.rlock.release()

        threading.Timer(self.INT_TTL, self.__update_registry_t).start()

    def updateRegistry(self, entry, loc="remote"):

        registry = self.registry

        e = entry
        a = []
        tep = []
        self.rlock.acquire()
        if e["name"] in registry.keys():
            log.debug("Updating peer: "+e["name"])
            #d is local registry
            #e is entry to update
            d = registry[e["name"]]
            epb = False
            for eep in e["endpoints"]:
                found = False
                for dep in d["endpoints"]:
                    if dep["ep"] == eep["ep"]:
                        found = True
                        self.__reset_ep_ttl(e["name"], eep["ep"])

                if not found:
                    if loc == "remote":
                        eep["priority"] = 10
                        self.__reset_ep_ttl(e["name"], eep["ep"])
                    d["endpoints"].append(eep)
                    log.info("Discovered remote seed: "+e["name"]+" at "+eep["ep"])
                    self.__reg_update = True
                    self.__display_graph = True
        else:
            log.info("Discovered new seed: "+e["name"]+" at "+e["endpoints"][0]["ep"])
            if loc == "remote":
                for ep in e["endpoints"]:
                    ep["priority"] = 10
                    self.__reset_ep_ttl(e["name"], ep["ep"])

            registry[e["name"]] = e
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
        #log.debug("Finding route for "+ tag)
        if tag in self.tagroute:
            return self.tagroute[tag]

    def getPriorityEndpoint(self, route):
        bep = None
        prt = 1000
        for ep in route['endpoints']:
            p = int(ep["priority"])
            if p < prt:
                bep = ep
                prt = p

        return bep

    def getExternalEndpoints(self, route):
        eps = []
        bep = None
        prt = 10
        pep = self.getPriorityEndpoint(route)
        for ep in route['endpoints']:
            p = int(ep["priority"])
            if p >= prt:
                if not ep["ep"] == pep["ep"]:
                    eps.append(ep)

        return eps

    def buildGraph(self):
        self.tagroute = {}
        G = self.graph
        for xo in self.registry.keys():
            eo = self.registry[xo]
            for isp in eo["istate"].split('|'):
                for osp in eo["ostate"].split('|'):
                    istype = eo["itype"]+":"+isp
                    if istype == "NONE:NONE":
                           istype = "INJECTION"
                    istype = eo["group"] +":"+ istype
                    ostype = eo["otype"]+":"+osp
                    if ostype == "NONE:NONE":
                        ostype = "EXTRACTION"
                    ostype = eo["group"] +":"+ ostype
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

    def dumpExternalRegistry(self):

        self.rlock.acquire()
        ne = copy.deepcopy(self.registry)
        self.rlock.release()

        for f in ne.values():
            #Filter out endpoints with less than priority 5. Any priority less than 5
            #is reserved for local communication such as IPC, INPROC, FILES
            f["endpoints"][:] = [x for x in f["endpoints"] if self.__determine(x)]

        d = json.dumps(ne)
        return d

    def __determine(self, ep):
        if int(ep["priority"]) < 5:
            return False
        else:
            return True

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
