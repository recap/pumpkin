__author__ = 'reggie'

import json
import Queue
import zmq
import time
import pika

import PmkSeed

from PmkShared import *
from Queue import *

class rx(Queue):
    def __init__(self):
        Queue.__init__(self)
        pass

class InternalDispatch(SThread):
    _packed_pkts = 0
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        pass

    def run(self):
        rx = self.context.getRx()
        while 1:
            pkts = rx.get(True)
            log.debug("Packet received: \n"+pkts)
            pkt = json.loads(pkts)
            #Check for PACK
            if pkt[0]["state"] == "PACK_OK":
                #log.debug("PACK packet: "+pkts)
                seed = pkt[0]["last_func"]

                if seed in PmkSeed.iplugins.keys():
                    klass = PmkSeed.iplugins[seed]
                    klass.pack_ok(pkt)
                    self._packed_pkts += 1
                    #log.debug("PACKED pkts: "+str(self._packed_pkts))
                    continue
            # if pkt[0]["state"] == "MERGE":
            #     seed = pkt[0]["last_func"]
            #
            #     if seed in PmkSeed.iplugins.keys():
            #         klass = PmkSeed.iplugins[seed]
            #         klass.merge(pkt)
            #         #klass.pack_ok(pkt)
            #         #self._packed_pkts += 1
            #         #log.debug("PACKED pkts: "+str(self._packed_pkts))
            #         continue
            if pkt[0]["state"] == "ARP_OK":
                log.debug("Received ARP_OK: "+json.dumps(pkt))
                self.context.put_pkt_in_shelve2(pkt)
                continue

            l = len(pkt)
            func = pkt[l-1]["func"]
            data = pkt[l-2]["data"]

            if ":" in func:
                func = func.split(":")[1]

            print "FUNC "+func


            if func in PmkSeed.iplugins.keys():
                klass = PmkSeed.iplugins[func]
                rt = klass._stage_run(pkt, data)




class Injector(SThread):
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context

    def run(self):
        for x in PmkSeed.iplugins.keys():
            klass = PmkSeed.iplugins[x]
            if not klass.hasInputs():
                #klass.run(klass.__rawpacket())
                klass.rawrun()

            if self.stopped():
                log.debug("Exiting thread "+self.__class__.__name__)
                break
            else:
                continue

class RabbitMQMonitor():
    class MonitorThread(SThread):
        def __init__(self, parent, context, connection, queue):
            SThread.__init__ (self)
            self.context = context

            host, port, username, password, vhost = self.context.get_rabbitmq_cred()
            credentials = pika.PlainCredentials(username, password)
            if connection == None:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,  credentials=credentials, virtual_host=vhost))
            else:
                self.connection = connection

            self.parent = parent
            self.tag_map = self.parent.tag_map
            self.channel = self.connection.channel()
            self.queue = queue
            self.cnt = 0
            self.channel.basic_qos(prefetch_count=1)
            self.channel.queue_declare(queue=str(queue), durable=True)
            #self.channel.basic_consume(self.callback,
            #          queue=queue,
            #          no_ack=True)


        def loop(self):
            while self.connection.is_open:

                try:
                    #FIX: bug trap empty queue
                    method, properties, body = self.channel.basic_get(queue=self.queue, no_ack=True)
                    if method:
                        if (method.NAME == 'Basic.GetEmpty'):
                            time.sleep(1)
                        else:
                            self.cnt += 1
                            log.debug("RabbitMQ received: "+ str(self.cnt))


                            pkt = json.loads(body)

                            l = len(pkt)
                            func = None
                            if method.routing_key in self.tag_map:
                                func = self.tag_map[method.routing_key]
                                if ":" in func:
                                    func = func.split(":")[1]
                                data = pkt[l-1]["data"]

                            if func in PmkSeed.iplugins.keys():
                                klass = PmkSeed.iplugins[func]
                                rt = klass._stage_run(pkt, data)
                    else:
                        time.sleep(1)
                except pika.exceptions.ConnectionClosed as e:
                    log.warning("Pika connection to "+self.queue+" closed.")



        # def callback(self, ch, method, properties, body):
        #     self.cnt += 1
        #     log.debug("RabbitMQ received: "+ str(self.cnt))
        #     pkt = json.loads(body)
        #     l = len(pkt)
        #     func = None
        #     if method.routing_key in self.tag_map:
        #         func = self.tag_map[method.routing_key]
        #         data = pkt[l-1]["data"]
        #
        #     if func in PmkSeed.iplugins.keys():
        #         klass = PmkSeed.iplugins[func]
        #         rt = klass._stage_run(pkt, data)

        def run(self):
            #self.channel.start_consuming()
            self.loop()

    def __init__(self, context, connection):
        self.connection = connection
        self.channel = connection.channel()
        self.context = context
        self.tag_map = {}

    def add_monitor_queue(self, queue, func):
        self.tag_map[queue] = func
        qthread = RabbitMQMonitor.MonitorThread(self, self.context, None, queue)
        qthread.start()

        #TODO:fix default queue
        # aqueue = queue.split(":")
        # if len(aqueue) > 2:
        #     queue2 = "T:"+aqueue[1]+":"+aqueue[2]
        #     self.tag_map[queue2] = func
        #
        #     # qthread = RabbitMQMonitor.MonitorThread(self, self.context, None, queue)
        #     # qthread.start()
        #
        #     try:
        #         self.channel = self.connection.channel()
        #         self.channel.queue_declare(queue=str(queue2), passive=True,durable=True)
        #         log.info("Using default rabbitmq queue: "+queue2)
        #         qthread = RabbitMQMonitor.MonitorThread(self, self.context, None, queue2)
        #         qthread.start()
        #     except Exception as e:
        #         qthread = RabbitMQMonitor.MonitorThread(self, self.context, None, queue)
        #         qthread.start()



        #self.channel.queue_declare(queue=queue)
        #self.channel.basic_consume(self.callback,
        #              queue=queue,
        #              no_ack=True)
        #self.channel.basic_qos(prefetch_count=10)

        #threading.Thread(target=self.channel.start_consuming)
        #self.channel.start_consuming()






class ZMQPacketMonitor(SThread):
    def __init__(self, context, zmqcontext, bind_to):
        SThread.__init__ (self)
        self.context = context
        self.bind_to = bind_to
        if (zmqcontext == None):
            self.zmq_cntx = zmq.Context()
            pass
        else:
            self.zmq_cntx = zmqcontext

        #self.zmq_cntx = zmq.Context()


        self.rx = self.context.getRx()

    def proccess_pkt(self, pkts):
        pkt = json.loads(pkts)
        log.debug("PACKET RECEIVED: "+pkts)
        #Check for PACK
        if pkt[0]["state"] == "PACK_OK":

            seed = pkt[0]["last_func"]

            if seed in PmkSeed.iplugins.keys():
                klass = PmkSeed.iplugins[seed]
                klass.pack_ok(pkt)
                self._packed_pkts += 1
                #log.debug("PACKED pkts: "+str(self._packed_pkts))
                return True
        # if pkt[0]["state"] == "MERGE":
        #     seed = pkt[0]["last_func"]
        #
        #     if seed in PmkSeed.iplugins.keys():
        #         klass = PmkSeed.iplugins[seed]
        #         klass.merge(pkt)
        #         #klass.pack_ok(pkt)
        #         #self._packed_pkts += 1
        #         #log.debug("PACKED pkts: "+str(self._packed_pkts))
        #         continue
        if pkt[0]["state"] == "ARP_OK":
            log.debug("Received ARP_OK: "+json.dumps(pkt))
            self.context.put_pkt_in_shelve2(pkt)
            return True

        l = len(pkt)
        func = pkt[l-1]["func"]
        data = pkt[l-2]["data"]

        if ":" in func:
                func = func.split(":")[1]

        if func in PmkSeed.iplugins.keys():
            klass = PmkSeed.iplugins[func]
            rt = klass._stage_run(pkt, data)


    def run(self):
        #context = zmq.Context()
        soc = self.zmq_cntx.socket(zmq.PULL)
        soc.setsockopt(zmq.RCVBUF, 2000)
        #soc.setsockopt(zmq.HWM, 5)
        soc.bind(self.bind_to)
        #soc.setsockopt(zmq.HWM, 1000)
        #soc.setsockopt(zmq.SUBSCRIBE,self.topic)
        #soc.setsockopt(zmq.RCVTIMEO, 10000)


        while True:
            try:
                msg = soc.recv()
                self.proccess_pkt(msg)
                del msg

                # if "REVERSE" in msg:
                #     log.debug(msg)
                #     ep = msg.split("::")[1]
                #     log.debug("Reverse connecting to: "+ep)
                #     rec = self.zmq_cntx.socket(zmq.PULL)
                #     rec.connect(ep)
                #     msg = rec.recv()
                #     log.debug("Received msg: "+msg)
                #     #continue
                #self.rx.put(msg)
                #log.debug("Message: "+str(msg))
            except zmq.ZMQError as e:
                if self.stopped():
                    log.debug("Exiting thread "+  self.__class__.__name__)
                    soc.close()
                    #zmq_cntx.destroy()
                    #zmq_cntx.term()
                    break
                else:
                    continue
            # except Exception as e:
            #     log.error(str(e))

            #except MemoryError as e:
            #    log.error(str(e))
            #    sys.exit(1)

        pass



#class InternalDispatch2(Thread):
#    def __init__(self, context):
#        Thread.__init__(self)
#        self.context = context
#        pass
#
#    def run(self):
#
#        rx = self.context.getRx()
#        tx = self.context.getTx()
#        while 1:
#            #fname = rx.get(True)
#            #fh = open(fname, "r")
#            #pkt = fh.read()
#            pkt = rx.get(True)
#            m = re.search('##START-CONF(.+?)##END-CONF(.*)', pkt, re.S)
#            if m:
#                pkt_header = m.group(1)
#                pkt_data = m.group(2)
#                d = json.loads(pkt_header)
#                for fc in d["invoke"]:
#                    state = fc["state"]
#                    if not ((int(state) & DRPackets.READY_STATE) == 1):
#                        func = fc["func"]
#                        log.debug("Trying invoking local function: "+str(func))
#                        if func in DRPlugin.hplugins:
#                            klass = DRPlugin.hplugins[func](self.context)
#                            #klass.on_load()
#                            rt = klass.run(pkt_data)
#                            pkt_data = rt
#                            #xf = klass()
#                            log.debug("RESULT: "+str(rt))
#                            fc["state"] = DRPackets.READY_STATE
#                            opkt = "##START-CONF" + json.dumps(d) + "##END-CONF\n"+str(rt)
#                            log.debug("Out PKT: "+ str(opkt))
#                            #tx.put(opkt)
#
#                            #foutname = "./tx/"+d["container-id"]+d["box-id"]+".pkt"
#                            #fout = open(foutname, "w")
#                            #fout.write(strg)
#                            #fout.flush()
#                            #fout.close()
#                            #log.debug("HERE 1")
#                            #tx.put(d,True)
#                            #log.debug("HERE 2")
#                            #break
#
#                            #log.debug("Return result: "+str(strg))
#                        else:
#                            log.debug("No local function "+func+" found")
#
#
#                    else:
#                        log.debug("Ready moving on")
#
#
#
#                #log.debug("Packet dispatch: "+str(pkt_header))




