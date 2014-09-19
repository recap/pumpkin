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


class tx(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)
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
        ep = pkt[0]["last_contact"]
        #TODO BUG BUG BUG seg faults due to sharing zmq shit between threads - solved with redispatcher

        if ep in self.redispatchers.keys():
            disp = self.redispatchers[ep]
            disp.dispatch(json.dumps(pkt))
        else:
            #disp = self.gdisp
            disp = ZMQPacketDispatch(self.context, self.context.zmq_context)
            if not disp == None:
                self.redispatchers[ep] = disp
                disp.connect(ep)
                disp.dispatch(json.dumps(pkt))
                #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

            else:
                logging.error("No dispatchers found for: "+ep)

        pass

    def send_express(self, otag, pkt):
        ntag = None
        state = pkt[0]["state"]

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
                     logging.debug("Found Routes: "+json.dumps(routes))
                     break
                else:
                    # dump non routable packets as this will lead to deadlock from tx queue filling up

                    #if self.context.is_speedy():
                    #    break
                    logging.debug("No Route: "+str(otag))
                    time.sleep(5)

            if routes:

                for r in routes:

                    if not self.context.is_speedy():

                        if len(routes) > 1:
                            dcpkt = copy.copy(pkt)
                        else:
                            dcpkt = pkt

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
                                    if aux & BROADCAST_BIT:
                                        pep_ar = r["endpoints"]
                                        header["aux"] = aux & (~BROADCAST_BIT)
                                    else:
                                        pep_ar = self.ep_sched.pick_route(r)

                        else:
                            pep_ar = self.ep_sched.pick_route(r)

                        if len(pep_ar) == 0:
                            logging.debug("No Route...")
                            found = False
                            continue

                        for pep in pep_ar:
                            #if len(pep_ar) > 1:
                            dcpkt2 = copy.deepcopy(dcpkt)


                            #print json.dumps(dcpkt2)

                            oep = self.context.get_our_endpoint(self.getProtoFromEP(pep["ep"]))
                            dcpkt2[0]["last_contact"] = oep[0]

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


                                if ep in self.dispatchers.keys():
                                    disp = self.dispatchers[ep]
                                    disp.dispatch(dcpkt2)
                                    #disp.dispatch(json.dumps(dcpkt))
                                    #disp.dispatch("REVERSE::tcp://192.168.1.9:4569::TOPIC")

                                else:
                                    disp = None
                                    if entry["mode"] == "zmq.PULL":
                                        print "Dispatch: PULL"
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



class EndpointPicker(object):
    def __init__(self, context):
        self.context = context
        self.route_index = {}

    def is_local_ext_ep(self, ep):
        if (int(ep["priority"]) >= 5) and (ep["cuid"] == self.context.getUuid()):
            return True
        return False

    def pick_route_exc(self, route, traces):
        ex_eps = traces.keys()

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


    def pick_route(self, route):
        route_id = route["name"]
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
        self.soc = self.zmq_cntx.socket(zmq.PUSH)
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

        print "SENDING MSG: "+json.dumps(pkt)

        message = zlib.compress(json.dumps(pkt))
        self.soc.send(message)

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

    def dispatch(self, pkt):
        send = False
        message = zlib.compress(json.dumps(pkt))
        while not send:
            try:
                if not self.connection.is_closed:
                    logging.debug("Sending pkt to rabbitmq")
                    self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)
                    send = True
                else:
                    self.connect(None)
                    logging.debug("Sending pkt to rabbitmq")
                    self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)
                    send = True
            except:
                time.sleep(1)
        pass

    def dispatch_bak(self, pkt):

        message = zlib.compress(json.dumps(pkt))
        logging.debug("Sending pkt to rabbitmq")
        self.channel.basic_publish(exchange='',routing_key=str(self.queue),body=message)

        pass

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
