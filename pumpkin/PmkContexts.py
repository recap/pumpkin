__author__ = 'reggie'

import sys
import shelve
import os
import shutil
import imp
import PmkSeed
import re
import logging
import json

import PmkShared

from PmkExternalDispatch import *
from PmkInternalDispatch import *
from PmkMonitor import *
from PmkBroadcast import *


from PmkProcessGraph import *

class MainContext(object):

    __instance = None

    def __init__(self,uuid, peer=None):
        if MainContext.__instance is None:
            MainContext.__instance = MainContext.__impl(uuid,peer)
         # Store instance reference as the only member in the handle
        self.__dict__['_MainContext__instance'] = MainContext.__instance

    def __getattr__(self, attr):
        """ Delegate access to implementation """
        return getattr(self.__instance, attr)

    def __setattr__(self, attr, value):
        """ Delegate access to implementation """
        return setattr(self.__instance, attr, value)

    class __impl:

        def __init__(self,uuid, peer=None):

            self.__pumpkin = None
            self.__uuid = uuid
            #self.__peer = peer
            self.__attrs = None
            self.__supernodes = []
            self.__threads = []

            self.rx = None #rx(100000)
            self.tx = None #tx(100000)
            self.mx = None

            self.cmd = cmd()
            self.registry = {}
            self.__ip = "127.0.0.1"
            self.endpoints = []
            self.__reg_update = False
            self.rlock = threading.RLock()
            self.proc_graph = ProcessGraph(self)
            self.__exec_context = None
            self.zmq_context = None
            self.working_dir = "~/.pumpkin/"+self.__uuid
            self.file_dir = None
            self.external_dispatch = None
            self.openfiles = []
            self.peers = {}

            self.__rabbitmq = None
            self.__rabbitmq_cred = ()

            self.pkt_shelve_2 = None

            pass

        def fallback_rabbitmq(self):
            if self.__attrs.rabbitmq_fallback:
                return True
            else:
                return False
            pass

        def set_rabbitmq(self, rabbitmq):
            self.__rabbitmq = rabbitmq

        def set_rabbitmq_cred(self, host, port=5672, username=None, password=None, virtual_host="soa"):
            self.__rabbitmq_cred = (host, port, username, password, virtual_host)

        def get_rabbitmq_cred(self):
            return self.__rabbitmq_cred

        def get_rabbitmq(self):
            return self.__rabbitmq

        def with_acks(self):
            return self.__attrs.ack

        def with_shelve(self):
            return self.__attrs.persistent

        def is_ghost(self):
            return self.__attrs.ghost

        def is_speedy(self):
            return self.__attrs.gonzales

        def get_broadcast_rate(self):
            return int(self.__attrs.brate)

        def with_broadcast(self):
            return self.__attrs.broadcast

        def load_seed(self, file):

            _,tail = os.path.split(file)
            modname = tail[:-3]
            if( file[-2:] == "py"):
                logging.debug("Found seed: "+file)
                file_header = ""
                fh = open(file, "r")
                fhd = fh.read()
                m = re.search('##START-CONF(.+?)##END-CONF(.*)', fhd, re.S)

                if m:
                    conf = m.group(1).replace("##","")
                    if conf:
                        d = json.loads(conf)
                        if not "auto-load" in d.keys() or d["auto-load"] == True:
                            imp.load_source(modname,file)

                            klass = PmkSeed.hplugins[modname](self)
                            PmkSeed.iplugins[modname] = klass
                            klass.pre_load(d)
                            klass.on_load()
                            klass.post_load()

            return modname

        def startPktShelve(self, filename):
            self.pkt_shelve = shelve.open(self.working_dir+"/"+filename)
            pass

        def startPktShelve2(self, filename):
            self.pkt_shelve_2 = shelve.open(self.working_dir+"/"+filename)
            pass


        def getPktId(self, pkt):
            id= pkt[0]["ship"]+":"+pkt[0]["container"]+":"+pkt[0]["box"]+":"+pkt[0]["fragment"]
            return id

        def pktReady(self, pkt):
            pkt_id = str(self.getPktId(pkt))
            self.pkt_shelve[pkt_id] = pkt
            pass

        def put_pkt_in_shelve(self, pkt):
            shlf = self.pkt_shelve
            pkt_id = str(self.getPktId(pkt))
            shlf[pkt_id] = pkt



        def put_pkt_in_shelve2(self, pkt):
            shlf = self.pkt_shelve_2
            pkt_id = str(self.getPktId(pkt))
            shlf[pkt_id] = pkt

        def get_pkt_from_shelve(self,pkt_id):
            pkt_id = str(pkt_id)
            pkt_id_parts = pkt_id.split(':')
            ret = []
            if len(pkt_id_parts) < 4:
                for k in self.pkt_shelve.keys():
                    if pkt_id in k:
                        ret.append(self.pkt_shelve[k])
            else:
                if pkt_id in self.pkt_shelve.keys():
                    spkt = self.pkt_shelve[pkt_id]
                    ret.append(spkt)

            return ret

        def get_pkt_from_shelve2(self,pkt_id):
            pkt_id = str(pkt_id)
            pkt_id_parts = pkt_id.split(':')
            ret = []
            if len(pkt_id_parts) < 4:
                for k in self.pkt_shelve_2.keys():
                    if pkt_id in k:
                        ret.append(self.pkt_shelve_2[k])
            else:
                if pkt_id in self.pkt_shelve_2.keys():
                    spkt = self.pkt_shelve_2[pkt_id]
                    ret.append(spkt)

            return ret


        def isPktShelved(self, pkt):

            pkt_id = str(self.getPktId(pkt))

            if pkt_id in self.pkt_shelve:
                spkt = self.pkt_shelve[pkt_id]
                #Check if the duplicate ID is for the same function
                #it could be a returning packet for a different function
                last_func = spkt[0]["last_func"]
                func_to_call = pkt[len(pkt) - 1]["func"]
                if last_func == func_to_call:
                    return True
            return False

        def get_pkt_shelve(self):
            return self.pkt_shelve

        def setExternalDispatch(self, dispatch):
            self.external_dispatch = dispatch
            pass

        def getExternalDispatch(self):
            return self.external_dispatch

        def setFileDir(self, file_dir):
            self.file_dir = file_dir
            pass

        def getFileDir(self):
            return self.file_dir

        def getFileServerEndPoint(self):
            ep = "tftp://"+self.get_local_ip()+":"+str(TFTP_FILE_SERVER_PORT)
            return ep

        def getWorkingDir(self):
            return self.working_dir

        def setExecContext(self, cntx):
            self.__exec_contex = cntx
            pass

        def getExecContext(self):
            return self.__exec_contex

        def set_local_ip(self,ip):
            self.__ip = ip



            #FIXME dynamic configuration
            #self.endpoints.append( ("ipc://"+self.getUuid(), "zmq.ipc", "zmq.PULL" ) )
            #self.endpoints.append( ("ipc://"+self.getUuid(), "zmq.ipc", "zmq.PUB" ) )
            #self.endpoints.append(("tcp://"+str(ip)+":"+str(ZMQ_ENDPOINT_PORT), "zmq.tcp", "zmq.PULL"))

            pass



        #def getEndpoint(self):
        #    #if self.__attrs.eptype == "zmq.TCP":
        #    #    return "tcp://"+str(self.__ip)+":"+str(ZMQ_ENDPOINT_PORT)
        #    #if self.__attrs.eptype == "zmq.IPC":
        #    #    return "ipc:///tmp/"+self.getUuid()
        #    return "inproc://"+self.getUuid()


        def isZMQEndpoint(self, entry):
            e = str(entry[1])
            if "zmq" in e.lower():
                return True
            return False

        def getEndpoints(self):
            return self.endpoints

        def get_group(self):
            return self.__attrs.group

        def is_with_nocompress(self):
            if self.__attrs.nocompress:
                return True
            else:
                return False

        def hasRx(self):
            return self.__attrs.rxdir

        def singleSeed(self):
            return self.__attrs.singleseed

        def get_local_ip(self):
            return self.__ip

        def set_public_ip(self, pip):
            self.__pip = pip

        def get_public_ip(self):
            return self.__pip

        def getProcGraph(self):
            return self.proc_graph

        def funcExists(self, func_name):
            if func_name in self.registry.keys():
                return True

            return False

        def getAttributeValue(self):
            return self.__attrs

        def get_our_endpoint(self, proto):
            tcp_ep = None
            h_ep = ("","","",0)
            for ep in self.endpoints:
                if ep[3] > h_ep[3]:
                    h_ep = ep
                if str(ep[0]).startswith(proto):
                    return ep
                #if str(ep[0]).startswith("tcp://"):
                #    tcp_ep = ep

            #logging.warning("Found no endpoint matching defaulting to tcp")
            return h_ep

        def get_our_pub_ep(self, proto="tcp"):
            ep = "tcp://"+str(self.get_local_ip())+":"+str(PmkShared.ZMQ_PUB_PORT)
            return ep

        def setEndpoints(self):
            if self.__attrs.eps == "ALL":
                #self.__attrs.eps = "tftp://*:*/*;inproc://*;ipc://*;tcp://*:*"
                #self.__attrs.eps = "inproc://*;tcp://*:*"
                #self.__attrs.eps = "amqp://*"
                #self.__attrs.eps = "tcp://*:*"
                self.__attrs.eps = "inqueue://*"

            #if self.fallback_rabbitmq():
            #    self.__attrs.eps += ";amqp://*"

            epl = self.__attrs.eps.split(";")
            for ep in epl:
                prts = ep.split("//")
                prot = prts[0]
                if prot == "inproc:":
                    if prts[1] == "*":
                        s = "inproc://"+self.getUuid()
                    else:
                        s = ep
                    self.endpoints.append( (s, "zmq.INPROC", "zmq.PULL", 1) )
                    logging.debug("Added endpoint: "+s)

                if prot == "inqueue:":
                    if prts[1] == "*":
                        s = "inqueue://"+self.getUuid()
                    else:
                        s = ep
                    self.endpoints.append( (s, "raw.Q", "raw.Q", 1) )
                    logging.debug("Added endpoint: "+s)

                if prot == "amqp:":
                    if prts[1] == "*":
                        s = "amqp://"+self.getUuid()
                    else:
                        s = ep
                    self.endpoints.append( (s, "amqp.PUSH", "amqp.PUSH", 15) )
                    logging.debug("Added endpoint: "+s)

                elif prot == "ipc:":
                    if prts[1] == "*":
                        s = "ipc:///tmp/"+self.getUuid()
                        self.openfiles.append("/tmp/"+self.getUuid())
                    else:
                        s = ep
                    self.endpoints.append( (s, "zmq.IPC", "zmq.PULL", 4) )
                    logging.debug("Added endpoint: "+s)

                elif prot == "tcp:":
                    addr = prts[1].split(":")
                    if addr[0] == "*":
                        addr[0] = self.__ip
                    if addr[1] == "*":
                        addr[1] = str(PmkShared.ZMQ_ENDPOINT_PORT)

                    s = "tcp://"+addr[0]+":"+addr[1]
                    self.endpoints.append( (s, "zmq.TCP", "zmq.PULL", 15) )
                    logging.debug("Added endpoint: "+s)

                #TODO uncomment once tftp is integrated
                #elif prot == "tftp:":
                #
                #    addrprts = prts[1].split("/")
                #    addr = addrprts[0].split(":")
                #    dir_path = addrprts[1]
                #    if addr[0] == "*":
                #        addr[0] = self.__ip
                #    if addr[1] == "*":
                #        addr[1] = str(TFTP_FILE_SERVER_PORT)
                #    if dir_path == "*":
                #        dir_path = "/tmp/tftproot/"
                #
                #    s = "tftp://"+addr[0]+":"+addr[1]+"/"+dir_path
                #    self.endpoints.append( (s, "tftp.UDP", "tftp.SERVER") )
                #    logging.debug("Added endpoint: "+s)
                else:
                    logging.warning("Unknown endpoint: "+ep)


        def log_to_file(self):
            logger = logging.getLogger()
            fh = logging.FileHandler(self.getWorkingDir()+"logs/pumpkin.log")
            fh.setLevel(logging.DEBUG)
            formatter = logging.Formatter("%(levelname)s - %(message)s")
            fh.setFormatter(formatter)
            logger.addHandler(fh)
            pass


        def is_debug(self):
            attributes = self.__attrs
            if attributes.debug:
                return True
            else:
                return False


        def set_attributes(self, attributes):
            self.__attrs = attributes
            logger = logging.getLogger()
            if attributes.debug:
                logger.setLevel(logging.DEBUG)
            else:
                logger.setLevel(logging.INFO)

            if self.__attrs.rabbitmq_host:
                a = self.__attrs
                self.set_rabbitmq_cred(host=a.rabbitmq_host, username=a.rabbitmq_user, password=a.rabbitmq_pass, virtual_host=a.rabbitmq_vhost)



            #self.setEndpoints()

            #epm = attributes.epmode
            #ept = attributes.eptype
            #prot = ept.split('.')[1].lower()
            #if prot == "tcp":
            #    s = prot+"://"+self.__ip+":"+str(ZMQ_ENDPOINT_PORT)
            #if prot == "ipc":
            #    s = prot+":///tmp/"+self.getUuid()
            #if prot == "inproc":
            #    s = prot+"://"+self.getUuid()
            #
            #self.endpoints.append( (s, ept, epm) )

        def getUuid(self):
            return self.__uuid

        def getPeer(self):
            return self.__peer

        def getTaskDir(self):
            return self.__attrs.taskdir

        def hasShell(self):
            return self.__attrs.shell

        def isSupernode(self):
            return self.__attrs.supernode

        def isWithNoPlugins(self):
            return self.__attrs.noplugins

        def setSupernodeList(self, sn):
            self.__supernodes = sn

        def getSupernodeList(self):
            return self.__supernodes

        def addThread(self, th):
            self.__threads.append(th)

        def getThreads(self):
            return self.__threads

        # def setMePeer(self, peer):
        #     self.__peer = peer
        #
        # def getMePeer(self):
        #     return self.__peer

        def get_stat(self):
            tm = time.time()
            ip = str(self.get_local_ip())
            guid = str(self.getUuid())

            rep = "["
            rep += '{"host_id": "'+guid+'", "timestamp" : '+str(tm)+',"ip" : "'+ip+'"},\n'

            jrep = {}
            total_in = 0
            total_out = 0
            total_pexec = 0
            total_npkts = 0
            for x in PmkSeed.iplugins.keys():
                klass = PmkSeed.iplugins[x]
                forecast = klass.get_forecast()
                jrep[klass.get_name()]={}
                jrep[klass.get_name()]["enabled"] = klass.is_enabled()
                jrep[klass.get_name()]["stags"] = klass.get_state_counters()
                jrep[klass.get_name()]["npkts"] = forecast[0]
                jrep[klass.get_name()]["msize"] = forecast[1]
                jrep[klass.get_name()]["pexec"] = forecast[2]
                tin, tout = klass.get_all_counters()
                total_in += tin
                total_out += tout
                total_pexec += forecast[2]
                total_npkts += forecast[0]

            #jreps = json.dumps(jrep, separators=(',',':') )
            jreps = json.dumps(jrep)

            rep += jreps
            rep += ","

            rep += '\n'
            rep = rep + '{"total_in":'+str(total_in)+',"total_out":'+str(total_out)+'}'
            rep += ","

            #rx_size = context.get_rx_size()
            tx_size = self.get_tx_size()

            rep += '\n'
            rep += '{"rx_size" : "'+str(total_npkts)+'", "tx_size" : "'+str(tx_size)+'", "total_pexec": "'+str(total_pexec)+'"}'

            rep += "]"

            return rep


        def getRx(self):
            return self.rx

        def getTx(self):
            return self.tx

        def get_mx(self):
            return self.mx

        def get_cores(self):
            c = int(self.__attrs.cores)
            return c

        def get_rx_size(self):
            s = self.rx.qsize()
            return s

        def get_tx_size(self):
            s = self.tx.qsize()
            return s

        def get_cmd_queue(self):
            return self.cmd

        def close(self):
            self.pkt_shelve.close()
            for f in self.openfiles:
                os.remove(f)
            shutil.rmtree(self.getWorkingDir())

        def get_buffer_size(self):
            size = self.__attrs.bsize
            return size

        def start_rxtx_buffer(self):
            logging.info("Setting buffer queue limit to: "+str(self.get_buffer_size()))
            self.rx = rx(self.get_buffer_size())
            self.tx = tx(self.get_buffer_size())
            self.mx = mx()
            pass







