__author__ = 'reggie'

import ujson as json
import Queue
import zmq
import time
import pika
import sys

import PmkSeed

from PmkShared import *
from Queue import *

class rx(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)
        pass

    def dig(self, pkt):
        if (pkt[0]["state"] == "TRANSIT") or (pkt[0]["state"] == "NEW"):
            iplugins = PmkSeed.iplugins
            keys = PmkSeed.iplugins.keys
            l = len(pkt)
            func = pkt[l-1]["func"]
            #data = pkt[l-2]["data"]

            if ":" in func:
                func = func.split(":")[1]

            if func in keys():
                klass = iplugins[func]
                klass.look_ahead(pkt)

            pass

class InternalDispatch(SThread):
    _packed_pkts = 0
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        pass




    def run(self):
        rx = self.context.getRx()
        #loads = json.loads
        keys = PmkSeed.iplugins.keys
        iplugins = PmkSeed.iplugins
        speedy = self.context.is_speedy()
        while 1:
            #already in json format
            pkt = rx.get(True)

            #check if multiple packets
            if "multiple" in pkt[0].keys():
                logging.debug("Multiple packets")
                print json.dumps(pkt)
                if pkt[0]["state"] == "PACK_OK":
                    tm1 = time.time()
                    tm2 = float(pkt[0]["timestamp"])
                    tdelta = tm1 - tm2
                    mexec = float(pkt[0]["mexec"])

                    eff = mexec/tdelta
                    overhead = tdelta - mexec

                    st_tdelta = "{:.12f}".format(tdelta)
                    st_mexec = "{:.12f}".format(mexec)
                    st_overhead = "{:.12f}".format(overhead)
                    st_eff = "{:.12f}".format(eff)

                    print "PKT RPTTM: "+st_tdelta+" EXEC TIME: "+st_mexec+" OVERHEAD: "+st_overhead+" EFF: "+st_eff

                    for ipkt in pkt[0]["pkts"]:
                        seed = ipkt[0]["last_func"]

                        if seed in PmkSeed.iplugins.keys():
                            klass = PmkSeed.iplugins[seed]
                            klass.pack_ok(ipkt)
                            self._packed_pkts += 1
                    continue



                c = 0
                mpkt_t = time.time()
                for ipkt in pkt[0]["pkts"]:
                    c += 1
                    ipkt[0]["multiple"] = True
                    ipkt[0]["number"] = pkt[0]["number"]
                    ipkt[0]["seq"] = c
                    ipkt[0]["mexec"] = mpkt_t
                    ipkt[0]["timestamp"] = pkt[0]["timestamp"]
                    ##################invoke ipkt##########################
                    if not speedy:
                        #Check for PACK
                        if ipkt[0]["state"] == "PACK_OK":
                            #logging.debug("PACK packet: "+pkts)
                            seed = ipkt[0]["last_func"]

                            if seed in PmkSeed.iplugins.keys():
                                klass = PmkSeed.iplugins[seed]
                                klass.pack_ok(ipkt)
                                self._packed_pkts += 1
                                continue

                        if ipkt[0]["state"] == "ARP_OK":
                            logging.debug("Received ARP_OK: "+json.dumps(ipkt))
                            self.context.put_pkt_in_shelve2(ipkt)
                            continue
                    try:
                        l = len(ipkt)
                        func = ipkt[l-1]["func"]
                        data = ipkt[l-2]["data"]

                        if ":" in func:
                            func = func.split(":")[1]


                        if func in keys():
                            klass = iplugins[func]
                            if speedy:
                                rt = klass._stage_run_express(ipkt, data)
                            else:
                                rt = klass._stage_run(ipkt, data)
                    except:
                        logging.error("Unexpected error invoking function:", sys.exc_info()[0])



                    #########################end invoke ipkt###########################

                continue

            ##################invoke pkt##########################
            if not speedy:
                #Check for PACK
                if pkt[0]["state"] == "PACK_OK":
                    #logging.debug("PACK packet: "+pkts)
                    seed = pkt[0]["last_func"]


                    if seed in PmkSeed.iplugins.keys():
                        klass = PmkSeed.iplugins[seed]
                        klass.pack_ok(pkt)
                        self._packed_pkts += 1
                        #logging.debug("PACKED pkts: "+str(self._packed_pkts))

                        tm = time.time()
                        tdelta = tm - float(pkt[0]["last_timestamp"])
                        print "TM 0: "+str(tm)+" TM 1: "+pkt[0]["last_timestamp"]
                        pexec = float(pkt[0]["pexec"])
                        overhead = tdelta - pexec
                        eff = pexec / tdelta
                        ieff = tdelta / pexec
                        st_tdelta = "{:.12f}".format(tdelta)
                        st_pexec = "{:.12f}".format(pexec)
                        st_overhead = "{:.12f}".format(overhead)
                        st_eff = "{:.12f}".format(eff)
                        st_ieff = "{:.12f}".format(ieff)
                        print "PKT RPTT: "+st_tdelta+" EXEC TIME: "+st_pexec+" OVERHEAD: "+st_overhead+" EFF: "+st_eff+","+st_ieff

                        l = len(pkt)
                        last = pkt[l-1]
                        key = last["ep"]+"::"+last["func"]

                        self.context.update_eff(key, eff)

                        continue

                # if pkt[0]["state"] == "MERGE":
                #     seed = pkt[0]["last_func"]
                #
                #     if seed in PmkSeed.iplugins.keys():
                #         klass = PmkSeed.iplugins[seed]
                #         klass.merge(pkt)
                #         #klass.pack_ok(pkt)
                #         #self._packed_pkts += 1
                #         #logging.debug("PACKED pkts: "+str(self._packed_pkts))
                #         continue
                if pkt[0]["state"] == "ARP_OK":
                    logging.debug("Received ARP_OK: "+json.dumps(pkt))
                    self.context.put_pkt_in_shelve2(pkt)
                    continue

            l = len(pkt)
            func = pkt[l-1]["func"]
            data = pkt[l-2]["data"]

            if ":" in func:
                func = func.split(":")[1]


            #if func in PmkSeed.iplugins.keys():
            #    klass = PmkSeed.iplugins[func]
            #    rt = klass._stage_run(pkt, data)



            if func in keys():
                klass = iplugins[func]
                if speedy:
                    rt = klass._stage_run_express(pkt, data)
                else:
                    rt = klass._stage_run(pkt, data)


            #########################end invoke pkt###########################




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
                logging.debug("Exiting thread "+self.__class__.__name__)
                break
            else:
                continue

class RabbitMQMonitor():
    class MonitorThread(SThread):
        def __init__(self, parent, context, connection, queue, exchange=''):
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
            self.exchange = exchange
            self.cnt = 0
            self.channel.basic_qos(prefetch_count=1)
            #self.channel.exchange_declare(exchange=str(exchange), type='fanout')
            #self.channel.queue_declare(queue=str(queue))
            #self.channel.queue_bind(exchange=str(exchange),
            #       queue=str(queue))
            self.channel.queue_declare(queue=str(queue), durable=False, exclusive=True)
            #self.channel.basic_consume(self.callback,
            #          queue=queue,
            #          no_ack=True)




        def loop(self):
            rx = self.context.getRx()
            while self.connection.is_open:

                try:
                    #FIX: bug trap empty queue
                    method, properties, body = self.channel.basic_get(queue=self.queue, no_ack=True)
                    if method:
                        if (method.NAME == 'Basic.GetEmpty'):
                            time.sleep(1)
                        else:
                            self.cnt += 1
                            logging.debug("RabbitMQ received from "+self.queue+": "+ str(body))
                            pkt = json.loads(body)
                            #rx.dig(pkt)
                            rx.put(pkt)
                            # pkt = json.loads(body)
                            #
                            # l = len(pkt)
                            # func = None
                            # if method.routing_key in self.tag_map:
                            #     func = self.tag_map[method.routing_key]
                            #     if ":" in func:
                            #         func = func.split(":")[1]
                            #     data = pkt[l-1]["data"]
                            #
                            # if func in PmkSeed.iplugins.keys():
                            #     klass = PmkSeed.iplugins[func]
                            #     rt = klass._stage_run(pkt, data)
                    else:
                        time.sleep(1)
                except pika.exceptions.ConnectionClosed as e:
                    logging.warning("Pika connection to "+self.queue+" closed.")



        # def callback(self, ch, method, properties, body):
        #     self.cnt += 1
        #     logging.debug("RabbitMQ received: "+ str(self.cnt))
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

    def add_monitor_queue(self, queue, func=None):
        self.tag_map[queue] = func
        #fqueue = queue+":"+self.context.getUuid()+":"+func
        fqueue = queue
        qthread = RabbitMQMonitor.MonitorThread(self, self.context, None, fqueue, exchange='')
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
        #         logging.info("Using default rabbitmq queue: "+queue2)
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
        logging.debug("PACKET RECEIVED: "+pkts)
        #Check for PACK
        if pkt[0]["state"] == "PACK_OK":

            seed = pkt[0]["last_func"]

            if seed in PmkSeed.iplugins.keys():
                klass = PmkSeed.iplugins[seed]
                klass.pack_ok(pkt)
                self._packed_pkts += 1
                #logging.debug("PACKED pkts: "+str(self._packed_pkts))
                return True
        # if pkt[0]["state"] == "MERGE":
        #     seed = pkt[0]["last_func"]
        #
        #     if seed in PmkSeed.iplugins.keys():
        #         klass = PmkSeed.iplugins[seed]
        #         klass.merge(pkt)
        #         #klass.pack_ok(pkt)
        #         #self._packed_pkts += 1
        #         #logging.debug("PACKED pkts: "+str(self._packed_pkts))
        #         continue
        if pkt[0]["state"] == "ARP_OK":
            logging.debug("Received ARP_OK: "+json.dumps(pkt))
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
        #soc.setsockopt(zmq.HWM, 100)
        soc.bind(self.bind_to)
        #soc.setsockopt(zmq.HWM, 1000)
        #soc.setsockopt(zmq.SUBSCRIBE,self.topic)
        #soc.setsockopt(zmq.RCVTIMEO, 10000)

        queue_put = self.context.getRx().put
        dig = self.context.getRx().dig
        while True:
            try:
                msg = soc.recv()
                #self.context.getRx().put(msg)
                pkt = json.loads(msg)
                if "multiple" not in pkt[0].keys():
                    dig(pkt)
                queue_put(pkt)
                #self.proccess_pkt(msg)
                #del msg

                # if "REVERSE" in msg:
                #     logging.debug(msg)
                #     ep = msg.split("::")[1]
                #     logging.debug("Reverse connecting to: "+ep)
                #     rec = self.zmq_cntx.socket(zmq.PULL)
                #     rec.connect(ep)
                #     msg = rec.recv()
                #     logging.debug("Received msg: "+msg)
                #     #continue
                #self.rx.put(msg)
                #logging.debug("Message: "+str(msg))
            except zmq.ZMQError as e:
                if self.stopped():
                    logging.debug("Exiting thread "+  self.__class__.__name__)
                    soc.close()
                    #zmq_cntx.destroy()
                    #zmq_cntx.term()
                    break
                else:
                    continue
            # except Exception as e:
            #     logging.error(str(e))

            #except MemoryError as e:
            #    logging.error(str(e))
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
#                        logging.debug("Trying invoking local function: "+str(func))
#                        if func in DRPlugin.hplugins:
#                            klass = DRPlugin.hplugins[func](self.context)
#                            #klass.on_load()
#                            rt = klass.run(pkt_data)
#                            pkt_data = rt
#                            #xf = klass()
#                            logging.debug("RESULT: "+str(rt))
#                            fc["state"] = DRPackets.READY_STATE
#                            opkt = "##START-CONF" + json.dumps(d) + "##END-CONF\n"+str(rt)
#                            logging.debug("Out PKT: "+ str(opkt))
#                            #tx.put(opkt)
#
#                            #foutname = "./tx/"+d["container-id"]+d["box-id"]+".pkt"
#                            #fout = open(foutname, "w")
#                            #fout.write(strg)
#                            #fout.flush()
#                            #fout.close()
#                            #logging.debug("HERE 1")
#                            #tx.put(d,True)
#                            #logging.debug("HERE 2")
#                            #break
#
#                            #logging.debug("Return result: "+str(strg))
#                        else:
#                            logging.debug("No local function "+func+" found")
#
#
#                    else:
#                        logging.debug("Ready moving on")
#
#
#
#                #logging.debug("Packet dispatch: "+str(pkt_header))




