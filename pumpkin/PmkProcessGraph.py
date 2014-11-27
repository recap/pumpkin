__author__ = 'reggie'

import ujson as json
import networkx as nx
import time
import thread
import threading
import copy
import re

from networkx.readwrite import json_graph

from PmkShared import *
from PmkPacket import *
from PmkEndpoint import *

class ProcessGraph(object):

    MAX_TTL = 60 #seconds
    INT_TTL = 15 #seconds
    def __init__(self, context):

        self.registry = {}
        self.external_registry = {}
        self.__reg_update = False
        self.__display_graph = False
        self.rlock = threading.RLock()
        self.graph = nx.DiGraph()
        self.ep_graph = nx.DiGraph()
        self.func_graph = nx.DiGraph()
        self.tagroute = {}
        self.hostroute = {}
        self.ttl = {}
        self.context = context

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
                logging.info("Removed stale entry for seed "+name+" at "+ep)
                del e["endpoints"][c]




    def __update_registry_t(self):
        self.rlock.acquire()
        keys_for_removal = []
        for key in self.ttl.keys():
            self.ttl[key] -= self.INT_TTL
            if self.ttl[key] <= 0:
                self.__del_ep(key)
                self.__reg_update = True
                keys_for_removal.append(key)


        for rk in keys_for_removal:
            del self.ttl[rk]

        if self.__reg_update == True:
            self.graph = self.buildGraph()

        self.rlock.release()

        threading.Timer(self.INT_TTL, self.__update_registry_t).start()

    def dumpRoutingTable(self):
        return json.dumps(self.tagroute)

    def __remove_same_public_ep(self, eps):
        rk = -1
        for x in range(0, len(eps)):
            ep = eps[x]
            ip = Packet.get_ip_from_ep(ep["ep"])
            if ip == self.context.get_public_ip():
               rk = x

        if rk >= 0:
            del eps[rk]

        return eps

    def updateRegistry(self, entry, loc="remote"):

        registry = self.registry

        e = entry
        a = []
        tep = []
        self.rlock.acquire()
        if loc == "remote":
            self.__remove_same_public_ep(e["endpoints"])
        if e["name"] in registry.keys():
            logging.debug("Updating peer: "+e["name"])
            #d is local registry
            #e is entry to update
            d = registry[e["name"]]
            epb = False
            for eep in e["endpoints"]:
                found = False
                for dep in d["endpoints"]:
                    if dep["ep"] == eep["ep"]:
                        if("cpu" in dep.keys()) and ("cpu" in eep.keys()):
                            if int(float(dep["cpu"])) != int(float(eep["cpu"])):
                                dep["cpu"] = eep["cpu"]
                                self.__reg_update = True

                        found = True
                        self.__reset_ep_ttl(e["name"], eep["ep"])


                if not found:
                    if loc == "remote":

                        eep["priority"] = int(eep["priority"]) - 5
                        self.__reset_ep_ttl(e["name"], eep["ep"])
                        eep["state"] = Endpoint.NEW_STATE
                        eep["tracer_burst"] = 0
                        eep["tracer_interval"] = Endpoint.TRACER_INTERVAL
                    d["endpoints"].append(eep)
                    logging.info("Discovered remote seed: "+e["name"]+" at "+eep["ep"])
                    self.__reg_update = True
                    self.__display_graph = True
        else:


            if loc == "remote":
                #remove public ip from internal docker nodes
                #self.__remove_same_public_ep(e["endpoints"])

                for ep in e["endpoints"]:
                    logging.info("Discovered new seed: "+e["name"]+" at "+ep["ep"])
                    ep["priority"] =  int(ep["priority"]) - 5
                    ep["state"] = Endpoint.NEW_STATE
                    ep["tracer_burst"] = 0
                    ep["tracer_interval"] = Endpoint.TRACER_INTERVAL
                    self.__reset_ep_ttl(e["name"], ep["ep"])

            else:
                for ep in e["endpoints"]:
                    ep["state"] = Endpoint.OK_STATE
                    logging.info("Discovered local seed: "+e["name"]+" at "+ep["ep"])


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
        self.rlock.acquire()
        ret = None
        #logging.debug("Finding route for "+ tag)
        if tag in self.tagroute.keys():
            ret = self.tagroute[tag]

        self.rlock.release()
        return ret

    def getPriorityEndpoint(self, route):
        bep = None
        prt = 1000
        for ep in route['endpoints']:
            p = int(ep["priority"])
            if p < prt:
                bep = ep
                prt = p

        return bep

    def stopSeed(self, seed):
        self.rlock.acquire()
        seedf = self.context.get_group()+":"+seed
        if seedf in self.registry.keys():
            entry = self.registry[seedf]
            entry["enabled"] = "False"
            self.__reg_update = True
            self.__display_graph = True

        if self.__reg_update == True:
            self.graph = self.buildGraph()
        self.rlock.release()
        return True

    def startSeed(self, seed):
        self.rlock.acquire()
        seedf = self.context.get_group()+":"+seed
        if seedf in self.registry.keys():
            entry = self.registry[seedf]
            entry["enabled"] = "True"
            self.__reg_update = True
            self.__display_graph = True

        if self.__reg_update == True:
            self.graph = self.buildGraph()
        self.rlock.release()
        return True

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

    def get_ep_with_priority(self, eps, priority):
        p = priority
        lep = None
        for ep in eps:
            if int(ep["priority"]) >= p:

                lep = ep
                return lep

        return lep


    def buildGraph(self):
        self.tagroute = {}
        self.graph = nx.DiGraph()
        self.ep_graph = nx.DiGraph()
        self.func_graph = nx.DiGraph()
        G = self.graph
        E = self.ep_graph
        F = self.func_graph


        for xo in self.registry.keys():
            eo = self.registry[xo]
            if (eo["enabled"] == "True") and (len(eo["endpoints"]) > 0):
                #for isp in eo["istate"].split('|'):
                #    for osp in eo["ostate"].split('|'):
                for isp in re.split('\||\&', eo["istate"]):
                    for osp in re.split('\||\&', eo["ostate"]):

                        istype = eo["itype"]+":"+isp
                        if istype == "NONE:NONE":
                               istype = eo["name"]+":INJECTION"
                        else:
                            istype = eo["group"] +":"+ istype
                        ostype = eo["otype"]+":"+osp
                        if ostype == "NONE:NONE":
                            ostype = eo["name"]+":EXTRACTION"
                        else:
                            ostype = eo["group"] +":"+ ostype
                        lep = self.get_ep_with_priority(eo["endpoints"], 0)
                        s_id= istype+":"+ostype
                        if lep:
                            G.add_edge(istype, ostype, function=eo["name"], ep=lep["ep"], id=s_id)
                        else:
                            G.add_edge(istype, ostype, function=eo["name"], id=s_id)

                    if istype in self.tagroute.keys():
                        self.tagroute[istype].append(eo)
                    else:
                        self.tagroute[istype] = []
                        self.tagroute[istype].append(eo)

                    for ep in eo["endpoints"]:
                        cuid = ep["cuid"]
                        if "wait" not in ep.keys():
                            ep["wait"] = 0
                        if "wshift" not in ep.keys():
                            ep["wshift"] = 0
                        if cuid in self.hostroute.keys():
                            if ep not in self.hostroute[cuid]:
                                self.hostroute[cuid].append(ep)
                        else:
                            self.hostroute[cuid] = []
                            self.hostroute[cuid].append(ep)


        if not self.context.is_speedy():
            for edge in G.edges(data=True):
                n1 = edge[0]
                n2 = edge[1]

                n1_routes = []
                n2_routes = []
                n1_trans = []
                n2_trans = []
                n2_name = None
                n1_name = None
                #if n1 in self.tagroute.keys() and "TRACE" not in n1:
                if n1 in self.tagroute.keys():
                    n1_routes = self.tagroute[n1][0]["endpoints"]
                    #n1_name = self.tagroute[n1][0]["name"].split(":")[1]+"()"
                    n1_trans = self.tagroute[n1]

                #if n2 in self.tagroute.keys() and "TRACE" not in n2:
                if n2 in self.tagroute.keys():
                    n2_routes = self.tagroute[n2][0]["endpoints"]
                    #n2_name = self.tagroute[n2][0]["name"].split(":")[1]+"()"
                    n2_trans = self.tagroute[n2]

                if "TRACE" in n1:
                    for n1s in n1_routes:
                        if not "cpu" in n1s.keys():
                            n1s["cpu"] = "0"
                        #E.add_node(n1s["ep"], ip= n1s["ip"], public_ip=n1s["pip"], attrs=n1s["attrs"], cpu=n1s["cpu"])
                        E.add_node(n1s["cuid"], ip= n1s["ip"], public_ip=n1s["pip"], attrs=n1s["attrs"], cpu=n1s["cpu"])


                if "TRACE" in n2:
                    for n2s in n2_routes:
                        if not "cpu" in n2s.keys():
                            n2s["cpu"] = "0"

                        #E.add_node(n2s["ep"], ip= n2s["ip"], public_ip=n2s["pip"], attrs=n2s["attrs"], cpu=n2s["cpu"])
                        E.add_node(n2s["cuid"], ip= n2s["ip"], public_ip=n2s["pip"], attrs=n2s["attrs"], cpu=n2s["cpu"])



                if "TRACE" not in n1 and "TRACE" not in n2:
                    for tr1 in n1_trans:
                        n1_routes = tr1["endpoints"]
                        n1_name = tr1["name"].split(":")[1]+"()"
                        for tr2 in n2_trans:
                            n2_routes = tr2["endpoints"]
                            n2_name = tr2["name"].split(":")[1]+"()"

                            for n1s in n1_routes:
                                for n2s in n2_routes:
                                    if not "cpu" in n1s.keys():
                                        n1s["cpu"] = 0
                                    #E.add_node(n1s["ep"], ip= n1s["ip"], public_ip=n1s["pip"], attrs=n1s["attrs"], cpu=n1s["cpu"])
                                    E.add_node(n1s["cuid"], ip= n1s["ip"], public_ip=n1s["pip"], attrs=n1s["attrs"], cpu=n1s["cpu"])

                                    if not "cpu" in n2s.keys():
                                        n2s["cpu"] = 0
                                    #E.add_node(n2s["ep"], ip= n2s["ip"], public_ip=n2s["pip"], attrs=n2s["attrs"], cpu=n2s["cpu"])
                                    E.add_node(n2s["cuid"], ip= n2s["ip"], public_ip=n2s["pip"], attrs=n2s["attrs"], cpu=n2s["cpu"])

                                    #e_id = n1s["ep"]+":"+n2s["ep"]
                                    #E.add_edge(n1s["ep"],n2s["ep"], id=e_id)

                                    e_id = n1s["cuid"]+":"+n2s["cuid"]
                                    E.add_edge(n1s["cuid"],n2s["cuid"], id=e_id)

                                    f_id = n1_name+":"+n2_name
                                    F.add_edge(n1_name, n2_name, id=f_id)


                self.dump_ep_graph_to_file("eps.json")
                self.dump_func_graph_to_file("funcs.json")
                self.dumpGraphToFile("states.json")


        return G

    def disable_host_eps(self, host):
        if host in self.hostroute.keys():
            for ep in self.hostroute[host]:
                print "DISABLING"
                ep["enabled"] = False

    def enable_host_eps(self, host):
        if host in self.hostroute.keys():
            for ep in self.hostroute[host]:
                ep["enabled"] = True

    def update_ep_prediction(self, pred, host, ctag):
        if ctag in self.tagroute.keys():
            for eps in self.tagroute[ctag]:
                for ep in eps["endpoints"]:
                    if host in ep["cuid"]:
                        if "locked" in ep.keys():
                            if not ep["locked"]:
                                ep["c_pred"] = pred
                                if "timestamp" not in ep.keys():
                                    ep["timestamp"] = time.time()

                        else:
                            ep["locked"] = False
                            ep["c_pred"] = pred
                            if "timestamp" not in ep.keys():
                                ep["timestamp"] = time.time()






        pass

    def showGraph(self):
        #import matplotlib.pyplot as plt
        #nx.draw(self.graph)
        #plt.show()
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

    def dump_ep_graph(self):
        d = json_graph.node_link_data(self.ep_graph)
        return json.dumps(d)

    def dump_func_graph(self):
        d = json_graph.node_link_data(self.func_graph)
        return json.dumps(d)

    def dumpGraphToFile(self, filename):
        d = json_graph.node_link_data(self.graph)
        json.dump(d, open(filename,'w'))

        pass

    def dump_ep_graph_to_file(self, filename):
        d = json_graph.node_link_data(self.ep_graph)
        json.dump(d, open(filename,'w'))

        pass

    def dump_func_graph_to_file(self, filename):
        d = json_graph.node_link_data(self.func_graph)
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
        tmp_keys = []
        for k in ne.keys():
            f = ne[k]
            #Filter out endpoints with less than priority 15. Any priority less than 5
            #is reserved for local communication such as IPC, INPROC, FILES and remote TCP
            f["endpoints"][:] = [x for x in f["endpoints"] if self.__determine(x)]
            if len(f["endpoints"]) == 0 or (f["enabled"] == "False"):
                tmp_keys.append(k)

        for rk in tmp_keys:
            del ne[rk]

        cpu_load = get_cpu_util()

        for r in ne.keys():
            eps = ne[r]["endpoints"]
            for ep in eps:
                ep["cpu"] = str(cpu_load)



        d = json.dumps(ne)
        return d

    def __determine(self, ep):
        if int(ep["priority"]) < 13:
            return False
        else:
            return True

    def printRegistry(self):
        for x in self.registry.keys():
            e = self.registry[x]
            logging.info("Name: " + e["name"])
            for p in e["zmq_endpoint"]:
                logging.info("Endpoint: "+p)
            logging.info("Itype: " + e["itype"])
            logging.info("Istate: "+e["istate"])
            logging.info("Otype: " + e["itype"])
            logging.info("Ostate: "+e["istate"])
