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
import threading
import math



from networkx.readwrite import json_graph


from Queue import *
from PmkShared import *


class tx(Queue):
    def __init__(self, maxsize=0, context=None):
        Queue.__init__(self, 1)
        self.rlock = Semaphore(0)
        pass

    def acquire(self):
        self.rlock.acquire()

    def release(self):
        try:
            self.rlock.release()
        except:
            pass





class ExternalDispatch(SThread):

    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        self.dispatchers = {}
        self.redispatchers = {}
        self.gdisp = ZMQPacketDispatch(self.context, self.context.zmq_context)
        #self.gdisp = ZMQPacketVentilate(self.context, self.context.zmq_context)




        self.graph = self.context.getProcGraph()
        self.tx = self.context.getTx()
        self.ep_sched = EndpointPicker(self.context)

        self._coll_queue = {}
        self._set_bunch = False
        self._bunch = 1
        self._pval =(0,0,0,0)

        self.bunches = []

        for i in range (1,10):
            v = i
            self.bunches.extend([v,v,v,v,v,v,v,v,v,v])

        for i in range (1, 100):
            v = i*50
            self.bunches.extend([v,v,v,v,v,v,v,v,v,v])

        self.bunches = self.bunches[::-1]

        pass

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
        ep.split("://")
        return ep[0]

    def send_to_ep(self, pkt, ep):

        if ep in self.redispatchers.keys():
            disp = self.redispatchers[ep]
            disp.dispatch(json.dumps(pkt))
        else:
            disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
            if not disp == None:
                self.redispatchers[ep] = disp
                disp.connect(ep)
                disp.dispatch(json.dumps(pkt))

            else:
                logging.error("No dispatchers found for: "+ep)

        pass
    def send_to_last(self, pkt):
        ep = pkt[0]["last_contact"]
        #TODO BUG BUG BUG seg faults due to sharing zmq shit between threads - solved with redispatcher

        self.select_n_send(pkt,ep)

        # if ep in self.redispatchers.keys():
        #     disp = self.redispatchers[ep]
        #     disp.dispatch(json.dumps(pkt))
        # else:
        #     #disp = self.gdisp
        #     disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
        #     if not disp == None:
        #         self.redispatchers[ep] = disp
        #         disp.connect(ep)
        #         disp.dispatch(json.dumps(pkt))
        #         #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")
        #
        #     else:
        #         logging.error("No dispatchers found for: "+ep)

        pass

    def select_n_send(self,pkt, ep=None, mode=None):
        if ep == None:
            ep = pkt[0]["last_contact"]




        disp = None
        if ep in self.redispatchers.keys():
            disp = self.redispatchers[ep]
            disp.dispatch(json.dumps(pkt))
        else:
            if mode == None:
                if "inproc" in ep:
                    mode = "zmq.PULL"
                if "tcp" in ep:
                    mode = "zmq.PULL"
                if "inqueue" in ep:
                    mode = "raw.Q"
                if "amqp" in ep:
                    mode = "amqp.PUSH"

            if mode == "zmq.PULL":
                disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                pass

            if mode == "amqp.PUSH":
                disp = RabbitMQDispatch(self.context)
                pass

            if mode == "raw.Q":
                disp = InternalRxQueue(self.context)
                pass

            if not disp == None:
                self.dispatchers[ep] = disp
                disp.connect(ep)
                disp.dispatch(pkt)
                #disp.dispatch(json.dumps(dcpkt))
                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")
            else:
                logging.error("No dispatchers found for: "+ep)

    def send_express(self, otag, pkt):
        ntag = None


        if pkt[1] and not self.context.is_speedy():
            #TODO: some nodes complain that loads does not exist
            #g = json_graph.loads(pkt[1])
            g = {}

            if otag in g:
                d = g[otag]

                if d:
                    ntag = d.keys()[0]



        # if self.context.fallback_rabbitmq():
        #     ep = str(otag)
        #     if ep in self.dispatchers.keys():
        #         disp = self.dispatchers[ep]
        #         disp.dispatch(json.dumps(pkt))
        #     else:
        #         disp = RabbitMQDispatch(self.context)
        #         self.dispatchers[ep] = disp
        #         disp.connect(ep)
        #         disp.dispatch(unicode(json.dumps(pkt)))
        #         s = json.dumps(pkt)
        #     return

        routes = None
        while 1:
            routes = self.graph.getRoutes(otag)
            if routes:
                 logging.debug("Found routes: "+json.dumps(routes))
                 break
            else:
                # dump non routable packets as this will lead to deadlock from tx queue filling up
                if self.context.is_speedy():
                    break
                time.sleep(5)



        if routes:
            for r in routes:

                if len(routes) > 1:
                    dcpkt = copy.copy(pkt)
                else:
                    dcpkt = pkt

                rtag = r["otype"]+":"+r["ostate"]
                if (ntag and ntag == rtag) or not ntag:

                    #pep = self.context.getProcGraph().getPriorityEndpoint(r)
                    ##eep = self.context.getProcGraph().getExternalEndpoints(r)

                    pep = self.ep_sched.pick_route(r)
                    if not pep:
                        logging.debug("No Route...")
                        continue
                    oep = self.context.get_our_endpoint(self.getProtoFromEP(pep["ep"]))
                    dcpkt[0]["last_contact"] = oep[0]

                    if pep:
                        entry = pep
                        ep = pep["ep"]
                        next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : pep["ep"] }
                        dcpkt.append(next_hop)

                        key = pep["ep"]+"::"+r["name"]
                        eff, n, peff, pn = self.context.get_eff(key)


                        if eff == 2:
                            time.sleep(1)
                        if eff < 1:

                            cq = self._coll_queue
                            logging.debug("Efficiency for signiture "+str(key)+" too low "+str(eff))
                            if key not in cq.keys():
                                logging.debug("Delaying packet with signiture: "+str(key))
                                cq[key] = []
                                cq[key].append(pkt)
                            else:
                                logging.debug("Delaying packet with signiture: "+str(key))
                                cq[key].append(pkt)

                            if not self._set_bunch:
                                if self._pval[0] != eff:

                                    peff_sub10 = (peff*0.05)
                                    peff_pos10 = (peff*0.05)
                                    eff_diff = math.fabs(eff - peff)

                                    # if eff_diff > peff_pos10:
                                    #     if eff >= (peff - (peff*0.10)):
                                    if eff > peff:

                                        self._bunch += 20
                                        # if self._bunch < 2000:
                                        #     self._bunch = self._bunch * 2 + 1
                                        # else:
                                        #     self._bunch += 200

                                        self._pval = (eff, n, peff, pn)
                                        self._set_bunch = True
                                    else:
                                        #if self._bunch > 500:
                                        self._bunch = int(self._bunch * 0.80) + 1
                                        self._pval = (eff, n, peff, pn)
                                        self._set_bunch = True

                            # #bunch = 1
                            # gradient = 0
                            # deff = 0
                            # dn = 0
                            # if not self._set_bunch:
                            #     if n == 1 or (eff == peff):
                            #         self._bunch = int(n / eff)
                            #         self._set_bunch = True
                            #     else:
                            #         deff = eff - peff
                            #         dn = n - pn
                            #         if dn > 0:
                            #             gradient = deff / dn
                            #         else:
                            #             gradient = 0
                            #         self._bunch = self._bunch + (dn*gradient)
                            #         self._set_bunch = True



                            #print "BUNCHING: eff, n, bunch "+str(eff)+","+str(n)+","+str(bunch)


                            #if self._bunch > 15000:
                            #    self._bunch = 15000
                            #self._bunch = 5000
                            # if not self._set_bunch:
                            #     self._bunch = self.bunches.pop()
                            #     self._set_bunch = True

                            if len(cq[key]) > self._bunch:
                                #print "BUNCH: "+str(self._bunch)
                                multi_pkt = []
                                multi_pkt.append({})

                                multi_pkt[0]["multiple"] = True
                                multi_pkt[0]["number"] = len(cq[key])
                                multi_pkt[0]["pkts"] = cq[key]
                                multi_pkt[0]["timestamp"] = "{0:.12f}".format(time.time())
                                multi_pkt[0]["state"] = "NEW"
                                multi_pkt.append(next_hop)
                                dcpkt = copy.deepcopy(multi_pkt)
                                cq[key] = []
                                #print "BUNCH, GRADIENT, EFF, PEFF: "+str(self._bunch)+","+str(0)+","+str(eff)+","+str(peff)
                                self._set_bunch = False

                            else:
                                continue
                        # else:
                        #     time.sleep(0.3)


                        dcpkt[0]["last_timestamp"] = "{0:.12f}".format(time.time())

                        if ep in self.dispatchers.keys():
                            disp = self.dispatchers[ep]
                            #disp.dispatch(json.dumps(dcpkt))
                            disp.dispatch(dcpkt)
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
                                disp.dispatch(dcpkt)
                                #disp.dispatch(json.dumps(dcpkt))
                                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                            else:
                                logging.error("No dispatchers found for: "+ep)
        pass

    def __loop_body(self):
        group, state, otype, pkt = self.tx.get(True)
        otag = group+":"+otype+":"+state
        self.send_express(otag, pkt)
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

    def run_old(self):
        graph = self.context.getProcGraph()
        tx = self.context.getTx()
        ep_sched = EndpointPicker(self.context)

        while True:
            group, state, otype, pkt = tx.get(True)
            #logging.debug("Tx message state: "+ state+" otype: "+otype+" data: "+str(pkt))
            otag = group+":"+otype+":"+state
            ntag = None
            if pkt[1]:
                g = json_graph.loads(pkt[1])

                if otag in g:
                    d = g[otag]

                    if d:
                        ntag = d.keys()[0]


            while 1:
                routes = graph.getRoutes(otag)
                logging.debug("Found routes: "+json.dumps(routes))
                if routes:
                    break
                else:
                    #logging.debug("No route found for: "+otag)
                    time.sleep(5)

            for r in routes:
                #logging.debug("Route: "+str(r))
                dcpkt = copy.copy(pkt)
                rtag = r["otype"]+":"+r["ostate"]
                if (ntag and ntag == rtag) or not ntag:
                    #TODO make it more flexible not bound to zmq
                    #pep = self.context.getProcGraph().getPriorityEndpoint(r)
                    ##eep = self.context.getProcGraph().getExternalEndpoints(r)
                    pep = ep_sched.pick_route(r)
                    if not pep:
                        logging.error("No Route...")
                        continue
                    oep = self.context.get_our_endpoint(self.getProtoFromEP(pep["ep"]))
                    dcpkt[0]["last_contact"] = oep[0]

                    if pep:
                        entry = pep
                        ep = pep["ep"]
                        #logging.debug("Route found for function "+r["name"]+": "+pep["ep"])
                        next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : pep["ep"] }
                        dcpkt.append(next_hop)
                        #pkt.remove( pkt[len(pkt)-1] )
                        #logging.debug(json.dumps(pkt))
                        try:
                            if ep in self.dispatchers.keys():
                                disp = self.dispatchers[ep]
                                disp.dispatch(json.dumps(dcpkt))
                                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                            else:
                                disp = None
                                if entry["mode"] == "zmq.PULL":
                                    disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                                    #disp = ZMQPacketDispatch(self.context)
                                    #disp = self.gdisp

                                if not disp == None:
                                    self.dispatchers[ep] = disp
                                    disp.connect(ep)
                                    disp.dispatch(json.dumps(dcpkt))
                                    #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                                else:
                                    logging.error("No dispatchers found for: "+ep)
                        except Exception,e:

                            logging.error("Error sending packet, requeueing: "+e.message)
                            #Requeue
                            tx.put((group, state, otype, pkt))


                #if r["endpoints"][0]:
                #    entry = r["endpoints"][0]
                #    ep = r["endpoints"][0]["ep"]
                #    logging.debug(r["endpoints"][0]["ep"])
                #    next_hop = {"func" : r["name"], "stag" : otag, "exstate" : 0000, "ep" : r["endpoints"][0]["ep"] }
                #    dcpkt.append(next_hop)
                #    #pkt.remove( pkt[len(pkt)-1] )
                #    #logging.debug(json.dumps(pkt))
                #    if ep in self.dispatchers.keys():
                #        disp = self.dispatchers[ep]
                #        disp.dispatch(json.dumps(dcpkt))
                #    else:
                #        disp = None
                #        if entry["mode"] == "zmq.PULL":
                #            disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
                #            #disp = ZMQPacketDispatch(self.context)
                #
                #        if not disp == None:
                #            self.dispatchers[ep] = disp
                #            disp.connect(ep)
                #            disp.dispatch(json.dumps(dcpkt))
                #        else:
                #            logging.error("No dispatchers found for: "+ep)




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

    def is_local_ext_ep(self, ep):
        if (int(ep["priority"]) >= 5) and (ep["cuid"] == self.context.getUuid()):
            return True
        return False

    def pick_route(self, route):
        route_id = route["name"]
        no_entries = len(route["endpoints"])
        try:
            if no_entries == 0:
                return False

            logging.debug("Route Picker: "+route_id+" entries: "+str(no_entries))
            if no_entries == 1:
                return route["endpoints"][0]
            if route["remoting"] == False and no_entries > 0:
                return route["endpoints"][0]


            if not route_id in self.route_index.keys():
                self.route_index[route_id] = -1

            s_idx = self.route_index[route_id]
            while 1:
                s_idx += 1
                s_idx = s_idx % no_entries
                ep = route["endpoints"][s_idx]
                if not self.is_local_ext_ep(ep):
                    self.route_index[route_id] = s_idx
                    return ep
        except IndexError:
            return False
            pass

        #
        # for ep in route['endpoints']:
        #     if not self.is_local_ext_ep(ep):
        #         pkt_counter = 0
        #         if not ep["ep"] in self.route_index.keys():
        #             self.route_index[ep["ep"]] = 1
        #             return ep
        #
        #
        #         cuid = ep["cuid"]
        #         p = int(ep["priority"])
        #
        #
        #         if p < prt:
        #             bep = ep
        #             prt = p
        #


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
        self.tx = self.context.getTx()

        logging.debug("Created ZMQPacketDispatch")
        if (zmqcontext == None):
            logging.debug("Creating zmq context")
            self.zmq_cntx = zmq.Context()
        else:
            self.zmq_cntx = zmqcontext

    def __check_ep(self, ep):
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
        return True

    def connect(self, connect_to):
        #self.soc = self.zmq_cntx.socket(zmq.PUSH)
        self.soc = self.zmq_cntx.socket(zmq.REQ)
        self.ep = connect_to
        #self.soc.setsockopt(zmq.HWM, 1)
        #self.soc.setsockopt(zmq.SWAP, 2048*2**10)
        logging.debug("ZMQ connecting to :"+str(connect_to))
        self.soc.connect(connect_to)

    def dispatch(self, pkt):

        #try:
            #if not self.__check_ep(self.ep):
            #    raise Exception("Endpoint closed")
            #logging.debug("SENDING")
            #self.soc.send(pkt, zmq.NOBLOCK)


        #except zmq.ZMQError as e:
        #    raise

        header = pkt[0]
        #if header["state"] != "PACK_OK":
        #    self.tx.acquire()
        #    print "AQCUIRE 1"
        if header["state"] != "PACK_OK":
            if "timestamp" in pkt[0].keys():
                pkt[0]["timestamp"] = "{0:.12f}".format(time.time())
        #   print "SENDING TIMESTAMP: "+pkt[0]["timestamp"]

        message = zlib.compress(json.dumps(pkt))

        self.soc.send(message)
        #time.sleep(2)
        m = self.soc.recv()

        if header["state"] != "PACK_OK":
            self.tx.acquire()
            #print "Broke LOCK"
            #print "ACQUIRE 2"

        #print "R: "+str(m)

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


    def connect(self, connect_to):
        self.soc = self.zmq_cntx.socket(zmq.PUSH)
        logging.debug("ZMQ connecting to :"+str(connect_to))
        self.soc.connect(connect_to)
        self.soc.send("REVERSE::ipc://127.0.0.1:7777")

    def dispatch(self, pkt):

        try:
            time.sleep(3)
            logging.debug("Sending")
            self.sender.send(json.dumps(pkt))
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
        pass

    def dispatch(self, pkt):
        self.queue.put(pkt)
        pass

    def close(self):
        pass


class RabbitMQDispatch(Dispatch):
    def __init__(self, context):
        Dispatch.__init__(self)
        self.context = context
        self.connection = None
        self.channel = None
        self.otag = None
        logging.debug("Created RabbitMQDisptach")
        pass

    def __open_rabbitmq_connection(self):
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(frame_max=None,backpressure_detection=False,host=host, credentials=credentials, virtual_host=vhost, heartbeat_interval=20))
        #channel = self.connection.channel()
        return connection

    def connect(self, connect_to):
        if connect_to:
            self.queue = connect_to.split("://")[1]
        self.connection = self.__open_rabbitmq_connection()
        self.channel = self.connection.channel()
        #self.channel.exchange_declare(exchange=str(self.queue), type='fanout')
        #self.channel.queue_declare(queue=str(self.queue), durable=False)

    def dispatch(self, pkt):
        send = False

        message = zlib.compress(json.dumps(pkt))

        while not send:
            try:
                if not self.connection.is_closed:
                    logging.debug("Pub from RabbitMQ")
                    self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)

                    send = True
                else:
                    print "OPEN CONNECTION"
                    #time.sleep(1)
                    self.connect(None)
                    logging.debug("Pub from RabbitMQ")
                    self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)

                    send = True
            except:
                time.sleep(1)


    def close(self):
        self.connetion.close()



#class ExternalDispatch2(Thread):
#    def __init__(self, context):
#        Thread.__init__(self)
#        self.context = context
#        pass
#
#    def run(self):
#        tx = self.context.getTx()
#        logging.debug("HERE 3")
#        d = tx.get(True)
#        logging.debug("HERE 4")
#        foutname = "./tx/"+d["container-id"]+d["box-id"]+".pkt"
#        foutnames = d["container-id"]+d["box-id"]+".pkt"
#        for fc in d["invoke"]:
#            state = fc["state"]
#            if not ((int(state) & DRPackets.READY_STATE) == 1):
#                func = fc["func"]
#                logging.debug("Function "+func)
#                peer = self.context.getMePeer().getPeerForFunc(func)
#                if not peer == None:
#                    comm = peer.getTftpComm()
#                    logging.debug("Got comm")
#                    if not comm == None:
#                        logging.debug("Comm to "+comm.host+" "+str(comm.port))
#                        client = tftpy.TftpClient(str(comm.host), int(comm.port))
#                        logging.debug("Files: "+foutnames+" "+foutname)
#                        tmpf = open(foutname, "r")
#                        fstr = tmpf.read()
#                        logging.debug("File: "+fstr)
#                        tmpf.close()
#                        client.upload(foutnames.encode('utf-8'),foutname.encode('utf-8'))
#                        #client.upload("test.pkt","./tx/test.pkt")
#                        break
#                else:
#                    logging.warn("No peer found for function "+func)
