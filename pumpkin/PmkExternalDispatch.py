__author__ = 'reggie'



import ujson as json

import time
import Queue
import zmq
import copy
import re
import socket
import networkx as nx
import pika
import zlib


from networkx.readwrite import json_graph


from Queue import *
from PmkShared import *
from PmkPacket import *
from PmkEndpoint import *


class tx(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)
        pass

    def put_pkt(self,pkt):
        header = pkt[0]
        c_tag_p = header["c_tag"].split(":")
        if len(c_tag_p) == 3:
            group = c_tag_p[0]
            otype = c_tag_p[1]
            tag   = c_tag_p[2]
            self.put((group, tag, otype, pkt))
        else:
            logging.warning("Ignored queueing TX packet: wrong c_tag")



class ExternalDispatch(SThread):


    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        self.dispatchers = {}
        self.redispatchers = {}
        self.last_contacts = {}
        self.gdisp = ZMQPacketDispatch(self.context, self.context.zmq_context)
        #self.gdisp = ZMQPacketVentilate(self.context, self.context.zmq_context)

        self.graph = self.context.getProcGraph()
        self.tx = self.context.get_tx(1)
        self.tx2 = self.context.get_tx(2)
        self.ep_sched = EndpointPicker(self.context)

        if self.context.fallback_rabbitmq():
            #host, port, username, password, vhost = self.context.get_rabbitmq_cred()
            #credentials = pika.PlainCredentials(username, password)
            #self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials, virtual_host=vhost))
            self.connection = self.__open_rabbitmq_connection()
            self.channel = self.connection.channel()
            self.delared_queues = {}
            #self.channel.queue_declare(queue=self.queue)

        pass

    def __open_rabbitmq_connection(self):
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials, virtual_host=vhost))
        #channel = self.connection.channel()
        return connection

    def getProtoFromEP(self, ep):
        ep_a = ep.split("://")
        return ep_a[0]

    def get_mode_from_ep(self, ep):
        prot = self.getProtoFromEP(ep)
        mode = None
        if prot == "tcp":
            mode = "zmq.PULL"
            pass
        if prot == "inqueue":
            mode = "raw.Q"
            pass
        if prot == "amqp":
            mode = "amqp.PUSH"
            pass

        return mode

    def send_to_entry(self, pkt, entry):
        ep = entry["ep"]
        if ep in self.dispatchers.keys():
                disp = self.dispatchers[ep]
                disp.dispatch(pkt)
        else:
            disp = None
            if entry["mode"] == "zmq.PULL":
                disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                #disp = ZMQPacketDispatch(self.context)
                #disp = self.gdisp

            if entry["mode"] == "amqp.PUSH":
                disp = RabbitMQDispatch(self.context)
                pass

            if entry["mode"] == "raw.Q":
                disp = InternalRxQueue(self.context)
                pass

            if not disp == None:
                self.dispatchers[ep] = disp
                disp.connect(ep)
                disp.dispatch(pkt)



    def send_to_ep(self, pkt, ep):
        mode = self.get_mode_from_ep(ep)

        if ep in self.redispatchers.keys():
            disp = self.redispatchers[ep]
            disp.dispatch(pkt)
        else:
            disp = None
            if mode == "zmq.PULL":
                disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                #disp = ZMQPacketDispatch(self.context)
                #disp = self.gdisp

            if mode == "amqp.PUSH":
                disp = RabbitMQDispatch(self.context)
                pass

            if mode == "raw.Q":
                disp = InternalRxQueue(self.context)
                pass

            if not disp == None:
                self.redispatchers[ep] = disp
                disp.connect(ep)
                disp.dispatch(pkt)

            else:
                logging.error("No dispatchers found for: "+ep)

        pass


    def send_to_last(self, pkt):
        header = pkt[0]
        if "act" in header.keys():
            ep = header["act"]
        else:
            ep = header["last_contact"]

        if not ep:
            logging.warn("No last contact to send back packet")
            return

        #TODO BUG BUG BUG seg faults due to sharing zmq shit between threads - solved with redispatcher

        if ep in self.redispatchers.keys():
            disp = self.redispatchers[ep]
            disp.dispatch(pkt)
        else:
            #disp = self.gdisp
            if ep.startswith("tcp://"):
                disp = ZMQPacketDispatch(self.context, self.context.zmq_context)

            if ep.startswith("amqp://"):
                disp = RabbitMQDispatch(self.context)

            if not disp == None:
                self.redispatchers[ep] = disp
                disp.connect(ep)
                disp.dispatch(pkt)
                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

            else:
                logging.error("No dispatchers found for: "+ep)

        pass

    def send_to_random_one(self, pkt):
        entry = None
        header = pkt[0]


        tracer_tag = self.context.get_group()+":Internal:TRACE"
        routes = self.graph.getRoutes(tracer_tag)[0]["endpoints"]
        entry = self.ep_sched.pick_random(routes, header["traces"])

        if entry:
            print "Random send to: "+json.dumps(entry)
            self.send_to_entry(pkt, entry)
            return True
        else:
            print "No random host found."
            return False


    def send_express(self, tags, pkt):
        otag = tags[0]+":"+tags[2]+":"+tags[1]
        ntag = None
        state = pkt[0]["state"]
        header = pkt[0]

        if pkt[1] and not self.context.is_speedy():
            #TODO: some nodes complain that loads does not exist
            #g = json_graph.loads(pkt[1])
            g = {}

            if otag in g:
                d = g[otag]

                if d:
                    ntag = d.keys()[0]

        routes = None
        found = False
        while not found:
            while 1:
                routes = self.graph.getRoutes(otag)
                if routes:
                     #logging.debug("Found Routes: "+json.dumps(routes))
                     #print "HERE1"
                     break
                else:
                    self.tx.put((tags[0],tags[1],tags[2],pkt))
                    found = True
                    #time.sleep(1)
                    break
                    # dump non routable packets as this will lead to deadlock from tx queue filling
                    # if self.context.is_speedy():
                    #     found = True
                    #     break

                    #if "seeds" in header.keys():
                    #    tracer_tag = self.context.get_group()+":Internal:TRACE"
                    #    routes = self.graph.getRoutes(tracer_tag)

                    # if routes:
                    #     break
                    #
                    # logging.debug("No Route: "+str(otag))
                    # time.sleep(5)
            #print "HERE2"
            if routes:

                for r in routes:
                    #print "HERE3"


                    if len(routes) > 1:
                        dcpkt = copy.copy(pkt)
                    else:
                        dcpkt = pkt

                    if not self.context.is_speedy():
                        #print "HERE3.1"
                        header = dcpkt[0]
                        rtag = r["otype"]+":"+r["ostate"]
                        if (ntag and ntag == rtag) or not ntag:

                            pep_ar = []

                            if state == "REDISPATCH":
                                last = dcpkt[len(pkt) -1]
                                #ex_eps = last["ep"].split("|,|")
                                pep_ar, lstate = self.ep_sched.pick_route_exc(r, last["traces"])
                                dcpkt[0]["state"] = lstate
                            else:
                                aux = 0
                                if "aux" in header.keys():
                                    aux = header["aux"]

                                if aux & Packet.BROADCAST_BIT:
                                    pep_ar = r["endpoints"]
                                    header["aux"] = aux & (~Packet.BROADCAST_BIT)
                                else:
                                    if "seeds" in header.keys():
                                        pep_ar = self.ep_sched.pick_route(r, False, dcpkt)
                                    else:
                                        pep_ar = self.ep_sched.pick_route(r, True, dcpkt)

                    else:
                        # speedy gonzales
                        pep_ar = self.ep_sched.pick_route(r,True,dcpkt)

                    if len(pep_ar) == 0:
                        logging.debug("No Route...")
                        found = False
                        continue

                    for pep in pep_ar:
                        #print "HERE5"
                        if len(pep_ar) > 1:
                            dcpkt2 = copy.deepcopy(dcpkt)
                        else:
                            dcpkt2 = dcpkt

                        #dest_proto = Packet.get_proto_from_ep(pep["ep"])
                        #if dest_proto == "tcp":
                        #    dest_ip = Packet.get_ip_from_ep(pep["ep"])

                        if pep["ep"] not in self.last_contacts.keys():
                            oep = self.context.get_matching_endpoint(pep["ep"])
                            self.last_contacts[pep["ep"]] = oep
                        else:
                            oep = self.last_contacts[pep["ep"]]

                        #oep = self.context.get_our_endpoint(self.getProtoFromEP(pep["ep"]))
                        if oep:
                            dcpkt2[0]["last_contact"] = oep[0]
                        else:
                            dcpkt2[0]["last_contact"] = None

                        if pep:
                            found = True
                            entry = pep
                            ep = pep["ep"]

                            if state == "REDISPATCH":
                                last = dcpkt2[len(dcpkt2) -1]
                                last["ep"] = ep
                                pass
                            else:
                                next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : pep["ep"] }
                                dcpkt2.append(next_hop)

                            #print "HERE6"
                            if pep["state"] == Endpoint.NEW_STATE:

                                if pep["tracer_burst"] < Endpoint.TRACER_BURST:
                                    pep["tracer_burst"] += 1
                                    dcpkt2 = Packet.set_tracer_bits(dcpkt2)
                                else:
                                    pep["state"] = Endpoint.OK_STATE
                            else:
                                if "tracer_interval" in pep.keys():
                                    pep["tracer_interval"] -= 1
                                    if pep["tracer_interval"] <= 0:
                                        dcpkt2 = Packet.set_tracer_bits(dcpkt2)
                                        pep["tracer_interval"] = Endpoint.TRACER_INTERVAL



                            if ep in self.dispatchers.keys():
                                #print "HERE7"
                                disp = self.dispatchers[ep]
                                disp.dispatch(dcpkt2)
                                #disp.dispatch(json.dumps(dcpkt))
                                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                            else:
                                disp = None
                                if entry["mode"] == "zmq.PULL":
                                    disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                                    #disp = ZMQPacketDispatch(self.context)
                                    #disp = self.gdisp

                                if entry["mode"] == "amqp.PUSH":
                                    disp = RabbitMQDispatch(self.context)
                                    pass

                                if entry["mode"] == "raw.Q":
                                    disp = InternalRxQueue(self.context)
                                    pass

                                if not disp == None:
                                    self.dispatchers[ep] = disp
                                    disp.connect(ep)
                                    disp.dispatch(dcpkt2)
                                    #disp.dispatch(json.dumps(dcpkt))
                                    #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                                else:
                                    logging.error("No dispatchers found for: "+ep)

                            if state == "REDISPATCH" and found == True:
                                break
        pass

    def __loop_body(self):

        if not self.tx2.empty():
            group, state, otype, pkt = self.tx2.get(True, 1)
            header = pkt[0]
            ok = False
            if header["aux"] & Packet.CODE_BIT:
                ok = True
                if not self.send_to_random_one(pkt):
                    #requeue
                    self.tx2.put((None,None,None,pkt))

            if header["aux"] & Packet.BCKPRESSURE_BIT:
                ok = True
                self.send_to_last(pkt)

            if not ok:
                otag = group+":"+otype+":"+state
                self.send_express((group,state,otype), pkt)

        try:
            group, state, otype, pkt = self.tx.get(True, 5)
            otag = group+":"+otype+":"+state
            self.send_express((group,state,otype), pkt)
        except Empty:
            return

        #except Exception as e:
        #    logging.error("Error sending pkt")
        #    return

        # if self.tx2.empty():
        #     try:
        #         group, state, otype, pkt = self.tx.get(True, 5)
        #         otag = group+":"+otype+":"+state
        #         self.send_express(otag, pkt)
        #     except Empty:
        #         return
        #
        #     except:
        #         logging.error("Error sending pkt")
        #         return
        # else:
        #     logging.debug("Packet on priority queue!")
        #     try:
        #         group, state, otype, pkt = self.tx2.get(True, 1)
        #     except Empty:
        #         return
        #     header =  pkt[0]
        #     if header["aux"] & Packet.CODE_BIT:
        #         if not self.send_to_random_one(pkt):
        #             #requeue
        #             self.tx2.put(None,None,None,pkt)
        #     if header["aux"] & Packet.BCKPRESSURE_BIT:
        #         self.send_to_last(pkt)
        #     else:
        #         otag = group+":"+otype+":"+state
        #         self.send_express(otag, pkt)
        pass

    def run(self):
        #graph = self.context.getProcGraph()
        #tx = self.context.getTx()
        #ep_sched = EndpointPicker(self.context)

        #soc = self.zmq_context.socket(zmq.PULL)
        #soc.bind("inproc://internal-bus")

        while True:
            self.__loop_body()

            if self.stopped():
                logging.debug("Exiting thread "+self.__class__.__name__)
                for ep in self.dispatchers.keys():
                    disp = disp = self.dispatchers[ep]
                    disp.close()
                break
            else:
                continue



class EndpointPicker(object):
    def __init__(self, context):
        self.context = context
        self.route_index = {}
        self.r_table = {}
        self.ep_checks = {}

    def is_local_ext_ep(self, ep):
        if (int(ep["priority"]) >= 5) and (ep["cuid"] == self.context.getUuid()):
            return True
        return False

    def _restructure_table(self, route):
        rtable = {}
        route_id = route["name"]
        if not route_id in rtable:
            rtable[route_id] = {}
        for ep in route["endpoints"]:
            cuid = ep["cuid"]

            if "enabled" in ep.keys():
                enabled = ep["enabled"]
            else:
                enabled = True
            if not enabled:
                #logging.debug("Disabling route for: "+cuid)
                continue

            priority = str(ep["priority"])

            if cuid not in rtable[route_id]:
                rtable[route_id][cuid] = {}
            if priority not in rtable[route_id][cuid]:
                rtable[route_id][cuid][priority] = []

            rtable[route_id][cuid][priority].append(ep)
        #print json.dumps(rtable)
        return rtable

    def _get_priority_eps(self, rtable, cuid, p=0):
        #p=0 always chooses lowest priority
        #p = 0
        first = rtable.keys()[0]
        proutes = rtable[first][cuid]

        cp = 1000000
        found = False
        for ep_key in proutes.keys():
            if (int(ep_key) < cp) and (int(ep_key) > p):

                cp = int(ep_key)
                found = True
                if cp == 11:
                    pass

        if found:
            return (cp, proutes[str(cp)])
        else:
            return (0, None)

    # def getProtoFromEP(self, ep):
    #     ep_a = ep.split("://")
    #     return ep_a[0]
    #
    # def get_mode_from_ep(self, ep):
    #     prot = self.getProtoFromEP(ep)
    #     mode = None
    #     if prot == "tcp":
    #         mode = "zmq.PULL"
    #         pass
    #     if prot == "inqueue":
    #         mode = "raw.Q"
    #         pass
    #     if prot == "amqp":
    #         mode = "amqp.PUSH"
    #         pass
    #
    #     return mode

    def pick_random(self, routes, traces):
        #route_id = route["name"]
        #ret_peps = []
        #found = False
        # for route in routes:
        #     rtable = self._restructure_table(route)
        #     for r_key in rtable[0].keys():
        #         if r_key is self.context.getUuid():
        #             continue
        #
        #         #node = rtable[r_key]
        #         _, eps = self._get_priority_eps(rtable, r_key, 0)
        #         for ep in eps:
        #             if self._check_conn_ep(ep):
        #                 return ep
        cp = 10000
        rep = None
        our_cuid = self.context.getUuid()
        for ep in routes:
            if ep["priority"] < cp and ep["cuid"] not in traces:
                if self._check_conn_ep(ep):
                    cp = ep["priority"]
                    rep = ep

        return rep




    def pick_route_exc(self, route, traces):
        ex_eps = traces.keys()
        rtable = self._restructure_table(route)

        rep = []
        found = True
        load = 10000000
        lep = None
        for ep in route["endpoints"]:
            found = True
            for xep in ex_eps:

                if ep["ep"] == xep:
                    found = False
                    xload = traces[xep]["load"]
                    if xload < load:
                        load = xload
                        lep = ep
                    continue

            if found == True:
                rep.append(ep)
                return (rep, "REDISPATCH")

        if found == False:
            rep.append(lep)
            return  (rep, "NOROUTE")


    def _check_conn_ep(self, epl):

        mode = epl["mode"]
        ep = epl["ep"]
        if not epl["enabled"]:
            return False


        if ep in self.ep_checks.keys():
            return self.ep_checks[ep]
        else:
            disp = None
            if mode == "zmq.PULL":
                disp = ZMQPacketDispatch(self.context, self.context.zmq_context)

            if mode == "amqp.PUSH":
                disp = RabbitMQDispatch(self.context)
                pass

            if mode == "raw.Q":
                disp = InternalRxQueue(self.context)
                pass

            if not disp == None:
                self.ep_checks[ep] = disp.test(ep)
                if not self.ep_checks[ep]:
                    epl["enabled"] = False
                return self.ep_checks[ep]

        return False



    def pick_route(self, route, local=True, pkt=None):
        route_id = route["name"]
        ret_peps = []
        found = False
        rtable = self._restructure_table(route)
        if len(rtable.keys()) == 0:
            return []
        first = rtable.keys()[0]
        no_entries = len(rtable[first])

        if not route_id in self.route_index.keys():
                self.route_index[route_id] = -1

        s_idx = self.route_index[route_id]
        #for cuid in rtable[first]:
        while not found:
            p=0
            if no_entries == 1:
                s_idx = 0
            else:
                s_idx += 1

            if s_idx >= no_entries:
                s_idx = s_idx % no_entries
            cuid = rtable[first].keys()[s_idx]

            if not local:
                if cuid == self.context.getUuid():
                    continue

            while not found:
                p, eps = self._get_priority_eps(rtable, cuid, p)
                if eps:
                    for ep in eps:
                        if self._check_conn_ep(ep):
                            if "c_pred" in ep.keys():
                                #p chooses ep priority from _get_priority_eps, setting it to 0 forces rescan
                                p = 0
                                t1 = time.time()
                                t2 = ep["timestamp"]
                                et = t1 - t2

                                bklog = ep["wshift"]
                                bklog -= et
                                if bklog > 0:
                                    ep["locked"] = True
                                    logging.debug("BACKLOG: "+str(bklog))
                                    continue
                                else:
                                    ep["locked"] = False
                                    ep["wshift"] = 0


                                w = ep["wait"] #+ ep["wshift"]
                                #if ep["wshift"] > 10:
                                #    print "SHIFT: "+ str(ep["wshift"])
                                #ep["wshift"] = 0

                                w -= et
                                if w < 0:
                                    #w = 0
                                    pred = ep["c_pred"]
                                    m = pred[0] # m in y = mx + c
                                    c = pred[1] # c in y = mx + c
                                    b = pred[2] #total queue backlog
                                    x = pkt[0]["c_size"]
                                    y = m*x + c
                                    #print "Adding: "+str(y)
                                    #adding
                                    ep["wait"] = (y) #+ b
                                    logging.debug("WAIT: "+str(y)+" WSHIFT: "+str(b)+" X: "+str(x)+" M: "+str(m)+" C: "+str(c))
                                    #print "SETTING: "+str(b)
                                    ep["wshift"] = b
                                    pred[2] = 0
                                    ep["timestamp"] = t1




                                else:
                                    #logging.debug("Waiting: "+str(w))
                                    continue


                            self.route_index[route_id] = s_idx

                            ret_peps.append(ep)
                            found = True
                            break

                        else:
                            #no connection
                            continue

                else:
                    break

        return ret_peps



    def pick_route_old(self, route):
        route_id = route["name"]


        #print json.dumps(route)
        no_entries = len(route["endpoints"])
        ret_peps = []
        try:
            if no_entries == 0:
                return False

            logging.debug("Route Picker: "+route_id+" entries: "+str(no_entries))
            if no_entries == 1:
                ret_peps.append(route["endpoints"][0])
                return ret_peps
                #return route["endpoints"][0]
            if route["remoting"] == False and no_entries > 0:
                ret_peps.append(route["endpoints"][0])
                return ret_peps
                #return route["endpoints"][0]


            if not route_id in self.route_index.keys():
                self.route_index[route_id] = -1

            s_idx = self.route_index[route_id]
            while 1:
                s_idx += 1
                s_idx = s_idx % no_entries
                ep = route["endpoints"][s_idx]
                if not self.is_local_ext_ep(ep):
                    self.route_index[route_id] = s_idx
                    ret_peps.append(ep)
                    return ret_peps

        except IndexError:
            return False
            pass


class Dispatch(object):
    def connect(self, connect_to):
        pass
    def dispatch(self, pkt):
        pass
    def close(self):
        pass

class ZMQPacketPublish(Dispatch):
    def __init__(self, context, zmqcontext=None):
        Dispatch.__init__(self)
        self.context = context
        self.soc = None
        if (zmqcontext == None):
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

    def connect(self, connect_to):
        self.soc = self.zmq_cntx.socket(zmq.PUB)
        self.soc.bind(connect_to)
        return True

    def dispatch(self, pkt):
            message = zlib.compress(json.dumps(pkt))
            self.soc.send(message)

    def close(self):
        self.soc.close()

class ZMQPacketDispatch(Dispatch):


    def __init__(self, context, zmqcontext=None):
        Dispatch.__init__(self)
        self.context = context
        self.soc = None
        self.ep = None
        logging.debug("Created ZMQPacketDispatch")
        if (zmqcontext == None):
            logging.debug("Creating zmq context")
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

    def test(self, ep):
        parts = re.split('://|:', ep)

        if str(parts[0]).lower() == "tcp":
            logging.debug("Checking ep..."+parts[1]+" "+parts[2])
            sock = socket(AF_INET, SOCK_STREAM)
            result = sock.connect_ex((parts[1], int(parts[2])))
            if result == 0:
                logging.debug("ep open: "+ep)
                return True
            else:
                logging.debug("ep closed: "+ep)
                logging.warn("Detected closed ep: "+ep)
                return False
        return False

    def connect(self, connect_to):
        try:
            self.soc = self.zmq_cntx.socket(zmq.PUSH)
            self.ep = connect_to
            #self.soc.setsockopt(zmq.HWM, 1)
            #self.soc.setsockopt(zmq.SWAP, 2048*2**10)
            logging.debug("ZMQ connecting to :"+str(connect_to))
            self.soc.connect(connect_to)
            return True
        except Exception as e:
            return False


    def dispatch(self, pkt):

        #try:
        #    if not self.__check_ep(self.ep):
        #        raise Exception("Endpoint closed")

        #except Exception as e:
        #    logging.error("Failed to connect to: "+self.ep)
        #    return


        logging.debug("Sending message: "+json.dumps(pkt))

        message = zlib.compress(json.dumps(pkt))
        try:
            self.soc.send(message)
        except Exception as e:
            logging.error("Failed to connect to: "+self.ep)
            time.sleep(1)
            return

    def close(self):
        self.soc.close()

class ZMQPacketVentilate(Dispatch):

    def __init__(self, context, zmqcontext=None):
        Dispatch.__init__(self)
        self.context = context
        self.soc = None
        logging.debug("Created ZMQPacketVentilate")
        if (zmqcontext == None):
            logging.debug("Creating zmq context")
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

        self.sender = self.zmq_cntx.socket(zmq.PUSH)
        self.sender.bind("ipc://127.0.0.1:7777")

    def test(self, ep):
        return False


    def connect(self, connect_to):
        self.soc = self.zmq_cntx.socket(zmq.PUSH)
        logging.debug("ZMQ connecting to :"+str(connect_to))
        self.soc.connect(connect_to)
        self.soc.send("REVERSE::ipc://127.0.0.1:7777")

    def dispatch(self, pkt):

        try:
            time.sleep(3)
            logging.debug("Sending")
            message = zlib.compress(json.dumps(pkt))
            self.sender.send(message)
        except zmq.ZMQError as e:
            logging.error(str(e))

    def close(self):
        self.soc.close()

class InternalRxQueue(Dispatch):
    def __init__(self, context):
        Dispatch.__init__(self)
        self.context = context
        self.queue = self.context.getRx()
        pass

    def connect(self, queue):
        return True
        pass

    def dispatch(self, pkt):
        self.queue.put(pkt)
        pass

    def test(self, ep):
        return True

    def close(self):
        pass


class RabbitMQDispatch(Dispatch):
    def __init__(self, context):
        Dispatch.__init__(self)
        self.context = context
        self.connection = None
        self.channel = None
        self.otag = None
        logging.info("Created RabbitMQDisptach")
        pass

    def __open_rabbitmq_connection(self):
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials, virtual_host=vhost))
        #channel = self.connection.channel()
        return connection

    def connect(self, connect_to):
        self.queue = connect_to.split("://")[1]
        self.connection = self.__open_rabbitmq_connection()
        self.channel = self.connection.channel()
        #self.channel.exchange_declare(exchange=str(self.queue), type='fanout')
        #self.channel.queue_declare(queue=str(self.queue), durable=False)
        return True

    def test(self, ep):
        return True

    def dispatch(self, pkt):
        send = False
        message = zlib.compress(json.dumps(pkt))
        while not send:
            try:
                #if not self.connection.is_closed:
                if True:
                    logging.debug("Sending pkt to rabbitmq")
                    self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)
                    send = True
                else:
                    self.connect(None)
                    logging.debug("Sending pkt to rabbitmq")
                    self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)
                    send = True
            except Exception as e:
                logging.ERROR("RabbitMQ connection error: "+str(e.message))
                time.sleep(1)
        pass

    def dispatch_bak(self, pkt):

        message = zlib.compress(json.dumps(pkt))
        logging.debug("Sending pkt to rabbitmq")
        self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)

        pass

    def close(self):
        self.connection.close()



