__author__ = 'reggie'

import inspect
import uuid
import copy
import tarfile
import shutil
import tftpy
import time
import thread
import collections
import networkx as nx
import ujson as json
import json as sjson
import logging
import inspect
import zmq
from numpy import arange,array,ones,linalg

import PmkShared

from PmkShared import *
from PmkPacket import *
from PmkBroadcast import *
from PmkExternalDispatch import ExternalDispatch
from networkx.readwrite import json_graph


plugins = []
hplugins = {}
iplugins = {}

#PKT_NEWBOX = 01
#PKT_OLDBOX = 02
MORE_PKT = 1
LAST_PKT = 0
PKT_PROCESS_WINDOW = 100


class SeedType(type):
    def __init__(cls, name, bases, attrs):
        super(SeedType, cls).__init__(name, bases, attrs)

        #print(cls, name, cls.__module__)
        if name != "Seed":
            plugins.append(cls)
            #logging.debug("Adding: "+name)
            hplugins[name] = cls


class Seed(object):
    __metaclass__ = SeedType



    _pkt_counter_interval = 10.0


    def __init__(self, context, poi="Unset"):
        #logging.basicConfig(filename=context.getWorkingDir()+self.get_name()+".log", format='%(levelname)s:%(message)s', level=logging.DEBUG)
        f = logging.Formatter(fmt='%(levelname)s-%(name)s:%(asctime)s:%(message)s',datefmt='%H:%M:%S')
        self.logger = logging.getLogger(self.get_name())
        fh = logging.FileHandler(context.getWorkingDir()+"logs/"+self.get_name()+".log")
        fh.setFormatter(f)
        self.logger.addHandler(fh)
        if context.is_debug():
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

        self.logger.info("Initialised seed: "+self.get_name())

        self.context = context
        self.name = self.__class__.__name__
        self.poi = poi
        self.conf = None
        self.tftp_sessions = {}
        self.flight_pkts = {}
        self._lock_fpkts = threading.Lock()
        self._lock_in_fpkts = threading.Lock()
        self.in_flight_pkts = {}
        self.ttf = None
        self._lock_telemetrics = threading.Lock()
        self._pkt_counter = 0
        self._in_pkts = 0
        self._out_pkts = 0
        self._avg_pkt_proc_time = 1
        self._avg_ingress = 0
        self._avg_outgress = 0
        self._state_counter = {}
        self._complexity = {}
        self._complexity_record = True
        self._forecast_lock = threading.Lock()
        self._forecast = {}
        self._forecast["npkts"] = 0
        self._forecast["msize"] = 0
        self._forecast["pexec"] = 0
        self._q_in_data = 0

        self._in_and_list = []
        self._in_all_list = []
        self._out_all_list = []
        self._out_and_list = []


        self.__routine_checks_t()
        if self.context.with_acks():
            self.pkt_checker_t()
        pass


    def __routine_checks_t(self):
        self._lock_telemetrics.acquire()
        self._avg_ingress = 0

        if self._pkt_counter > 0:
            self._avg_ingress = float(self._pkt_counter / self._pkt_counter_interval)

        self.logger.debug(self.get_name()+" avg pkt ingress: "+str(self._avg_ingress)+" pkt/s  ["+str(self._pkt_counter)+"]["+str(len(self.flight_pkts))+"]")
        self._pkt_counter = 0

        lmsg = ""
        for s in self._state_counter.keys():
           pkts = self._state_counter[s]
           lmsg += s+" "+str(pkts)+";"
        if lmsg:
           self.logger.debug(lmsg)


        self._lock_telemetrics.release()

        threading.Timer(self._pkt_counter_interval, self.__routine_checks_t).start()


    def _blankpacket(self):
        pkt = []
        ship_id = self.context.getExecContext()
        cont_id =  str(uuid.uuid4())[:8]
        pkt.append({"ship" : ship_id, "container" : cont_id, "box" : '0', "fragment" : '0', "e" : 0, "state": "NEW", \
                    "c_tag" : "NONE:NONE", "ttl" : 'D', "aux": 0, "t_state":"None", "t_otype" : "None" ,"stop_func": "None",\
                    "last_contact" : "None", "last_func" : "None", "last_timestamp" : 0})

        g = {}

        pkt.append(g)

        return pkt

    # def __rawpacket(self):
    #     pkt = []
    #     ship_id = self.context.getExecContext()
    #     cont_id =  str(uuid.uuid4())[:8]
    #     pkt.append({"ship" : ship_id, "container" : cont_id, "box" : '0', "fragment" : '0', "e" : 0, "state": "NEW", \
    #                 "c_tag" : "NONE:NONE", "ttl" : 'D', "aux": 0, "t_state":"None", "t_otype" : "None" ,"stop_func": "None",\
    #                 "last_contact" : "None", "las"
    #                                          "t_func" : "None", "last_timestamp" : 0})
    #     #Place holder for data automaton
    #     g = nx.DiGraph()
    #     in_tags = self.get_in_tag_list()
    #     out_tags = self.get_out_tag_list()
    #
    #     for it in in_tags:
    #         for ot in out_tags:
    #             g.add_edge(it,ot,function=self.get_name())
    #
    #     #g.add_edge("DataString:RAW", "DataString:PROCESSED", function="processor")
    #
    #     d =  json_graph.node_link_data(g)
    #     ds = json.dumps(d)
    #
    #     pkt.append( ds )
    #     pkt.append( {"stag" : "RAW", "exstate" : "0001", "ep" : "local"} )
    #
    #     return pkt

    def __rawpacket(self):
        ship_id = self.context.getExecContext()
        cont_id =  str(uuid.uuid4())[:8]
        pkt = Packet.new_empty_packet(ship_id, cont_id)
        return pkt

    def get_new_box(self):
        return str(uuid.uuid4())[:8]


    def get_new_container(self):
        return str(uuid.uuid4())[:8]

    def rawrun(self):
        self.run(self.__rawpacket())
        pass

    def get_cont_id(self,pkt):
        return pkt[0]["container"]

    def get_ship_id(self, pkt):
        return pkt[0]["ship"]

    def get_state(self, pkt):
        return pkt[0]["state"]

    def _tar_to_gz(self, source, destination=None, suffix=None):
        src = self.context.getWorkingDir()+source
        dst = destination
        if destination == None:
            if suffix:
                dst = self.context.getWorkingDir()+source+"-"+suffix+".tar.gz"
            else:
                dst = self.context.getWorkingDir()+source+".tar.gz"
        else:
            dst = self.context.getWorkingDir()+destination
            dst = dst.replace("//","/")
            self._ensure_dir(dst)

            if suffix:
                dst = dst+source+"-"+suffix+".tar.gz"
            else:
                dst = dst+source+".tar.gz"


        t = tarfile.open(name=dst, mode='w:gz')
        t.add(src, os.path.basename(src))
        t.close()

        return dst

    def move_file_to_wd(self, file):
        filep = file.split("/")
        file_name = filep[len(filep)-1]
        dst = self.context.getWorkingDir()+"/"+file_name
        src = self.context.getWorkingDir()+"/"+file
        shutil.move(src,dst)
        return file_name

    def copy_file_to_wd(self, file, mode=0644):
        filep = file.split("/")
        file_name = filep[len(filep)-1]
        dst = self.context.getWorkingDir()+"/"+file_name
        src = file
        shutil.copy(src,dst)
        os.chmod(dst,mode)
        return file_name

    def get_relative_path(self, file):
        return "."+str(file)

    def _add_to_tar(self, file, tar, postfix=None, rename=None):
        wd = self.context.getWorkingDir()
        mems = self._untar_to_wd(tar)
        pfile = os.path.basename(file)
        if postfix:
            pfile = pfile+"-"+postfix

        dir = None
        base_dir = None
        for m in mems:
            path = wd + str(m.name)
            if os.path.isdir(path):
                dir = path
                base_dir = "/"+m.name
                break
        dst = dir+"/"+pfile
        src = wd+file
        shutil.copy(src,dst)

        afile = wd+file
        atar = wd+tar



        outf = self._tar_to_gz(base_dir, suffix=postfix)
        if not rename == None:
            atar = wd+rename+".tar.gz"
            shutil.move(outf,atar)
            outf = atar

        shutil.rmtree(dir)

        return outf


    def _untar_to_wd(self, source, destination=None, rename=None):
        fp = source

        if not os.path.isfile(fp):
            fp = self.context.getWorkingDir() +"/"+ source
            fp = fp.replace("//","/")

        if os.path.isfile(fp):
            if not destination:
                destination = self.context.getWorkingDir()
            t = tarfile.open(fp)
            z = t.getmembers()


            t.extractall(destination)
            t.close()
            if rename:
                for d in z:
                    if os.path.isdir(destination+d.name):
                        shutil.move(destination+d.name,destination+rename)

            new_dst = self.context.getWorkingDir()+str(z[0])
            return z
        else:
            logging.error("Input file not found ["+source+"]")


    def moveto_fileserver(self, filename):
        fullpath = self.context.getWorkingDir()+"/"+filename
        if os.path.exists(fullpath):
            dst = self.context.getFileDir()+"/"+filename
        pass


    def __endpoint_parts(self, ep):
        p1 = ep.split("://")
        p2 = p1[1].split("/")
        rpath = ""
        for p in range(1, len(p2)):
            rpath += "/"+p2[p]
        file = p2[len(p2)-1]
        p3 = p2[0].split(":")
        ip = p3[0]
        port = p3[1]
        return [ip, port, rpath, file]

    def ack_pkt(self, pkt):
        if self.context.with_acks():
            dpkt = copy.deepcopy(pkt)
            dpkt[0]["state"] = "PACK_OK"
            pkt_id = self.get_pkt_id(dpkt)
            self._lock_in_fpkts.acquire()
            self.context.pktReady(dpkt)
            if pkt_id in self.in_flight_pkts: del self.in_flight_pkts[pkt_id]
            self._lock_in_fpkts.release()
            if not pkt[0]["last_func"] == None:
                 exdisp = self.context.getExternalDispatch()
                 exdisp.send_to_last(dpkt)
            pass


    def pack_ok(self, pkt):
        pkt_id = self.get_pkt_id(pkt)
        if pkt[0]["last_func"] == self.__class__.__name__:
            self._lock_fpkts.acquire()
            if pkt_id in self.flight_pkts.keys():
                del self.flight_pkts[pkt_id]
            self._lock_fpkts.release()
        else:
            logging.warning("Trying to ACK packet from another function!")
        pass

    def is_duplicate(self, pkt):
        if(self.context.is_speedy()):
            return False
        aux = 0
        if "aux" in pkt[0].keys():
            aux = int(pkt[0]["aux"])
            if aux & Packet.FORCE_BIT:
                return False
            if aux & Packet.TRACER_BIT:
                return False

        pkt_id = self.get_pkt_id(pkt)
        self._lock_in_fpkts.acquire()

        if pkt_id in self.in_flight_pkts.keys():
            #print json.dumps(pkt)
            self._lock_in_fpkts.release()
            return True
        if self.context.isPktShelved(pkt):
            #print json.dumps(pkt)
            self._lock_in_fpkts.release()
            return True
        self._lock_in_fpkts.release()
        return False

    def inc_state_counter(self, state):
        self._lock_telemetrics.acquire()

        if state in self._state_counter.keys():
            self._state_counter[state] += 1
        else:
            self._state_counter[state] = 1

        self._lock_telemetrics.release()

    def get_state_counters(self):
        return self._state_counter
        #return sjson.dumps(self._state_counter, separators=(',',':') )

    def get_all_counters(self):
        total_in = 0
        total_out = 0
        for cnt in self._state_counter.keys():
            if "IN" in cnt:
                total_in += int(self._state_counter[cnt])
            if "OUT" in cnt:
                total_out += int(self._state_counter[cnt])

        return (total_in, total_out)


    def __inc_pkt_counter(self):
        self._lock_telemetrics.acquire()
        self._pkt_counter += 1
        self._lock_telemetrics.release()

    def __dec_pkt_counter(self):
        self._lock_telemetrics.acquire()
        self._pkt_counter -= 1
        self._lock_telemetrics.release()


    def _stage_run(self,pkt, *args):
        pkt_id = self.get_pkt_id(pkt)
        if self.pre_run(pkt, args):
            pstate =  pkt[0]["state"]
            if not self.is_duplicate(pkt):
                tstag = "IN:"+self.__class__.__name__+":"+pkt[0]["c_tag"]
                pkt[0]["state"] = "PROCESSING"

                #if self.context.with_shelve():
                #    self._lock_in_fpkts.acquire()
                #    shelve = self.context.get_pkt_shelve()
                #    shelve[str(pkt_id)] = pkt
                #    self._lock_in_fpkts.release()

                if self.context.with_acks():
                    self._lock_in_fpkts.acquire()
                    self.in_flight_pkts[pkt_id] = pkt
                    self._lock_in_fpkts.release()

                try:
                    self.__inc_pkt_counter()

                    nargs = []
                    if(args[0]):
                        #msg = str(args[0])
                        for msg in args[0].split('|,|'):

                            if ( (not (self.context.is_speedy())) and (msg.startswith("tftp://"))):
                                ip, port, rpath, file = self.__endpoint_parts(msg)
                                if ip in self.tftp_sessions.keys():
                                    client = self.tftp_sessions[ip]
                                else:
                                    client = tftpy.TftpClient(ip, int(port))
                                    self.tftp_sessions[ip] = client

                                wdf = self.context.getWorkingDir()+"/rx/"+file
                                client.download(file, wdf)
                                furl = "file://"+wdf
                                nargs.append(furl)
                                self.set_pkt_data(pkt,furl)
                            else:
                                nargs.append(msg)

                        for x in range (1,len(args)-1):
                            nargs.append(args[x])

                    if pstate == "MERGE":
                        self.merge(pkt,nargs)
                        return
                    else:
                        self._pkt_start_timing(pkt)
                        if self.is_fragment(pkt):
                            self.inc_state_counter(tstag)
                            self.run(pkt,nargs)
                        else:
                            if not self.split(pkt, nargs):
                                self.inc_state_counter(tstag)
                                self.run(pkt,nargs)
                        self._pkt_end_timing(pkt)
                        return


                except Exception as e:
                    logging.error(str(e))
                    self._lock_in_fpkts.acquire()

                    if pkt_id in self.in_flight_pkts.keys():
                        del self.in_flight_pkts[pkt_id]

                    self._lock_in_fpkts.release()
                    pass
            else:
                logging.debug("Duplicate packet received: "+pkt_id)
                pass

    def regression(self):
        complexity = self.get_complexity_list()
        if len(complexity) >= 1:
            fdlen = None
            ldlen = None
            rep = ""
            lreg_arry = []
            xi = []
            self.stop_recording()

            for dlen in complexity.keys():
                if (fdlen == None):
                    fdlen = dlen
                ldlen = dlen
                xi.append(dlen)
                lreg_arry.append(complexity[dlen][0])

            self.start_recording()

            A = array([ xi, ones(xi.__len__())])
            y = lreg_arry
            x_len = xi.__len__()
            y_len = y.__len__()

            #w = (m, c)
            #y = mx + c
            w = linalg.lstsq(A.T,y)[0]
        else:
            w = (0, 0)
        return w

    def look_ahead(self, pkt):
        header = pkt[0]
        l = len(pkt)
        if header["aux"] & Packet.TIMING_BIT:
            if "data" in pkt[l-2].keys():
                 data = pkt[l-2]["data"]
            else:
                 data = ""
            data_len = sys.getsizeof(data)
            self._q_in_data += data_len
            header["c_size"] = data_len

        # self._forecast_lock.acquire()
        # w = self.regression()
        # if w[0] == 0:
        #     self._forecast_lock.release()
        #     return
        # forecast = self._forecast
        # qpkts = forecast["npkts"]
        # msize = forecast["msize"]
        # pexec = forecast["pexec"]
        #
        # l = len(pkt)
        #
        # data = None
        # if "data" in pkt[l-2].keys():
        #     data = pkt[l-2]["data"]
        # else:
        #     data = ""
        #
        # data_len = sys.getsizeof(data)
        #
        # pred_time = data_len*w[0] + w[1]
        #
        # pexec += pred_time
        # if(qpkts == 0):
        #     qpkts += 1
        # msize = (float(msize)*qpkts + float(data_len)) / (qpkts)
        # qpkts += 1
        #
        # forecast["npkts"] = qpkts
        # forecast["msize"] = msize
        # forecast["pexec"] = pexec
        #
        # pkt[0]["aux"] = pkt[0]["aux"] | Packet.TIMING_BIT
        # pkt[0]["pexec"] = pred_time
        # pkt[0]["dsize"] = data_len
        # self._forecast_lock.release()
        pass

    def queue_prediction(self, model="linear"):
        if model == "linear":
            m,c = self.regression()
            y = m*float(self._q_in_data) + c

            return y


    def get_forecast(self):
        # self._forecast_lock.acquire()
        # forecast = self._forecast
        # qpkts = forecast["npkts"]
        # msize = forecast["msize"]
        # pexec = forecast["pexec"]
        # self._forecast_lock.release()
        # return (qpkts, msize, pexec)

        pass

    def adj_forecast(self, pkt):

        # if "aux" in pkt[0].keys():
        #     if pkt[0]["aux"] & Packet.TIMING_BIT:
        #         self._forecast_lock.acquire()
        #         ptime = pkt[0]["pexec"]
        #         dsize = pkt[0]["dsize"]
        #
        #         forecast = self._forecast
        #         forecast["pexec"] -= ptime
        #         #forecast["msize"] = (forecast["msize"] - dsize) / 2
        #         forecast["npkts"] -= 1
        #         self._forecast_lock.release()
        pass

    def _pkt_start_timing(self, pkt):
        header = pkt[0]
        l = len(pkt)
        if header["aux"] & Packet.TIMING_BIT:
            header["last_timestamp"] = time.time()
            data = None
            if "data" in pkt[l-2].keys():
                data = pkt[l-2]["data"]
            else:
                data = ""
            data_len = sys.getsizeof(data)
            self._q_in_data -= data_len
            header["c_size"] = data_len

    def _pkt_reset_timing(self,pkt):
        header = pkt[0]
        if header["aux"] & Packet.TIMING_BIT:
            #reset timing bit
            header["aux"] = header["aux"] & (~Packet.TIMING_BIT)
            header["c_size"] = 0
            header["last_timestamp"] = 0

    def _pkt_end_timing(self, pkt):
        header = pkt[0]
        if (header["aux"] & Packet.TIMING_BIT) and (header["last_timestamp"] != 0):
            #reset timing bit
            header["aux"] = header["aux"] & (~Packet.TIMING_BIT)
            htime = time.time()
            stime = header["last_timestamp"]
            etime = htime - stime
            data_len = header["c_size"]
            complexity = self._complexity
            if data_len in complexity.keys():
                t = complexity[data_len][0]
                n = complexity[data_len][1]
                avg = (etime + t*n) / (n+1)
                complexity[data_len][0] = avg
                complexity[data_len][1] = n+1
            else:
                complexity[data_len] = [etime,1]

            #str_etime = "{:.12f}".format(etime)
            #print "Time: "+str(data_len)+" "+str_etime


    def _stage_run_express(self,pkt, *args):
        pkt_id = self.get_pkt_id(pkt)
        header = pkt[0]
        pstate =  header["state"]

        tstag = "IN:"+self.name+":"+header["c_tag"]
        header["state"] = "PROCESSING"

        nargs = []
        if(args[0]):
            msg = args[0]
            nargs.append(msg)
        else:
            nargs.append("Empty")
            #for msg in args[0].split('|,|'):
            #    nargs.append(msg)

        self.inc_state_counter(tstag)

        self._pkt_start_timing(pkt)
        self.run(pkt,*nargs)
        self._pkt_end_timing(pkt)

        # if self._complexity_record:
        #     stime = hin()
        #     self.run(pkt,*nargs)
        #     htime = hin()
        #
        #     etime = htime - stime
        #
        #     if dlen in complexity.keys():
        #         t = complexity[dlen][0]
        #         n = complexity[dlen][1]
        #         avg = (etime + t*n) / (n+1)
        #         complexity[dlen][0] = avg
        #         complexity[dlen][1] = n+1
        #     else:
        #         complexity[dlen] = [etime,1]
        #
        # self.adj_forecast(pkt)

        pass

    def set_pkt_aux_bit(self, pkt, bit):
        header = pkt[0]
        header["aux"] = header["aux"] | bit
        return pkt

    def reset_pkt_aux_bit(self, pkt, bit):
        header = pkt[0]
        header["aux"] = header["aux"] & (~bit)
        return pkt

    def stop_recording(self):
        self._complexity_record = False

    def start_recording(self):
        self._complexity_record = True

    def get_complexity_list(self):
        return self._complexity

    def split(self, pkt, *args):
        return False


    def run(self, pkt, *args):
        pass

    def merge(self, pkt, *args):
        pass

    def getpoi(self):
        return self.poi

    def is_non_local(self):
        if "non-local" in self.conf.keys():
            if self.conf["non-local"] == True:
                return True
        return False

    def is_remoting(self):
        if "remoting" in self.conf.keys():
            if self.conf["remoting"] == True:
                return True
        return False

    def disable(self):
        if "enabled" in self.conf.keys():
            self.conf["enabled"] = False

    def enable(self):
        if "enabled" in self.conf.keys():
            self.conf["enabled"] = True


    def is_enabled(self):
        if "enabled" in self.conf.keys():
            if self.conf["enabled"] == True:
                return True
        return False

    def get_group(self):
        if "group" in self.conf.keys():
            return self.conf["group"]
        else:
            group = self.context.get_group()
            if group:
                return group
            else:
                return "public"

    def get_conf(self):
        return self.conf


    def set_conf(self, jconf):
        self.conf = jconf
        pass

    def pre_load(self, jconf):
        self.conf = jconf
        #if not "enabled" in self.conf.keys():
        self.conf["enabled"] = True

        if len(self.conf["parameters"]) > 0:
            for p in self.conf["parameters"]:
                type = p["type"]
                tag_list = p["state"].split("|")
                for t in tag_list:
                    if not "&" in t:
                        transition = self.get_group()+":"+type+":"+t
                        self._in_all_list.append(transition)
                    else:
                        and_list = t.split("&")
                        for a in and_list:
                            transition = self.get_group()+":"+type+":"+a
                            self._in_and_list.append(transition)
                            self._in_all_list.append(transition)
        else:
            self._in_all_list.append("NONE:NONE")



        if len(self.conf["return"]) > 0:
            for p in self.conf["return"]:
                type = p["type"]
                tag_list = p["state"].split("|")
                for t in tag_list:
                    if not "&" in t:
                        transition = self.get_group()+":"+type+":"+t
                        self._out_all_list.append(transition)
                    else:
                        and_list = t.split("&")
                        for a in and_list:
                            transition = self.get_group()+":"+type+":"+a
                            self._out_and_list.append(transition)
                            self._out_all_list.append(transition)

        else:
            self._out_all_list.append("NONE:NONE")

        pass

    def post_load(self):
        #if self.context.fallback_rabbitmq():
        #    rabbitmq = self.context.get_rabbitmq()
        #    if rabbitmq:
        #        for q in self.get_in_tag_list():
        #            logging.debug("Adding RabbitMQ monitor: "+str(q))
        #            rabbitmq.add_monitor_queue(q, self.__class__.__name__)
        pass

    def hasInputs(self):
        if len(self.conf["parameters"]) > 0:
            return True

        return False

    def pkt_checker_t(self):
        if self.context.with_acks():
            self.logger.debug("Starting pkt checker thread")
            interval = 30
            reset = 60
            self._lock_fpkts.acquire()
            for k in self.flight_pkts.keys():
                pkt = self.flight_pkts[k]
                s_ttl = str(pkt[0]["ttl"])
                if s_ttl != "D":
                    ttl = int(pkt[0]["ttl"])
                    ttl = ttl - interval
                    if ttl <= 0:
                        ttl = reset
                        logging.debug("Resending packet: "+str(pkt))
                        state = pkt[0]["t_state"]
                        otype = pkt[0]["t_otype"]
                        self.context.getTx().put((self.get_group(), state, otype, pkt))
                        pass
                    pkt[0]["ttl"] = str(ttl)

            self._lock_fpkts.release()
            threading.Timer(interval, self.pkt_checker_t).start()

        pass

    def getConfEntry(self):
        js = '{ "name" : "'+self.get_group()+':'+self.get_name()+'", \
        "group" : "'+self.get_group()+'",\
        "remoting" : "'+str(self.is_remoting())+'",\
        "enabled" : "'+str(self.is_enabled())+'",\
       "endpoints" : [ '+self.__getEps()+' ],' \
       ''+self.get_parameters()+',' \
       ''+self.getreturn()+'}'

        return js

    def get_last_stag(self, pkt):
        l = len(pkt) - 1
        stag = pkt[l]["stag"].split(":")
        l2 = len(stag) - 1
        return str(stag[l2])

    def _ensure_dir(self, f):
        if not f[len(f)-1] == "/":
                f = f +"/"
        d = os.path.dirname(f)
        if not os.path.exists(d):
            logging.debug(d + " does not exist, creating...")
            os.makedirs(d, mode=0775)
            os.chmod(d,0775)
        pass

    def __getEps(self):
        aep = ""
        for ep in self.context.endpoints:
            attrs = "None"
            if is_amazon(self.context.get_public_ip()):
                attrs = "ec2"
            s =  '{"attrs" : "'+attrs+'", "pip" : "'+self.context.get_public_ip()+'", "ip" : "'+self.context.get_local_ip()+'", "ep" : "'+ep[0]+'", "cuid" : "'+self.context.getUuid()+'", "type" : "'+ep[1]+'", "mode" : "'+ep[2]+'", "priority" : "'+str(ep[3])+'"}'
            aep = aep + s + ","
        aep = aep[:len(aep)-1]
        return aep

    def get_in_tag_list(self):
        return self._in_all_list

    def get_out_tag_list(self):
        return self._out_all_list

    def get_parameters(self):
        if len(self.conf["parameters"]) > 0:
            for p in self.conf["parameters"]:
                sret =  '"itype" : "'+p["type"]+'", "istate" : "'+p["state"]+'"'
            return sret
        return ' "itype" : "NONE", "istate" : "NONE" '

    def getreturn(self):
        if len(self.conf["return"]) > 0:
            for p in self.conf["return"]:
                sret =  '"otype" : "'+p["type"]+'", "ostate" : "'+p["state"]+'"'
            return sret
        return ' "otype" : "NONE", "ostate" : "NONE" '

    def fork_dispatch(self, pkt, msg, state):
        npkt = self.duplicate_pkt_new_container(pkt)
        self.dispatch(npkt, msg, state)
        pass

    def broadcast(self, pkt, msg, state):
        aux = 0
        if "aux" in pkt[0].keys():
            aux = pkt[0]["aux"]
        aux = aux | Packet.BROADCAST_BIT

        self.fork_dispatch(pkt, msg, state)
        pass

    def ifFile(self, msg):
        if msg.startswith("file://"):
            return True
        return False

    def fileparts(self,filepath):
        prts1 = filepath.split("://")
        prot = prts1[0]
        prts2 = prts1[1].split("/")
        path= ""
        file = ""
        for p in range(0,len(prts2)-1):
            path += prts2[p]+"/"

        path = path.replace("//","/")
        file = prts2[len(prts2)-1]
        apath = path+file
        rpath = path.replace(self.context.getWorkingDir(),"/")
        rpath = rpath+file


        return [prot,path,file,apath,rpath]

    def get_pkt_id(self, pkt):
        id = None
        if pkt[0]["state"] == "MERGE":
            id= pkt[0]["ship"]+":"+pkt[0]["container"]+":"+str(pkt[0]["box"])+":"+str(pkt[0]["fragment"])+":M"
        else:
            id= pkt[0]["ship"]+":"+pkt[0]["container"]+":"+str(pkt[0]["box"])+":"+str(pkt[0]["fragment"])

        return id

    def duplicate_pkt_new_container(self,pkt, shallow=False):
        lpkt = None
        if shallow:
            lpkt = copy.copy(pkt)
        else:
            lpkt = copy.deepcopy(pkt)

        cont = self.get_new_container()
        header = lpkt[0]
        header["container"] = cont
        header["aux"] = pkt[0]["aux"]
        return lpkt



    def get_pkt_fragment_no(self, pkt):
        lpkt = copy.deepcopy(pkt)
        header = lpkt[0]
        return int(header["fragment"])

    def fragment_pkt(self, pkt, frag_no):
        lpkt = copy.deepcopy(pkt)
        header = lpkt[0]
        header["fragment"] = str(frag_no)
        header["e"] = MORE_PKT
        return lpkt

    def last_fragment_pkt(self, pkt, frag_no):
        lpkt = copy.deepcopy(pkt)
        header = lpkt[0]
        header["fragment"] = str(frag_no)
        header["e"] = LAST_PKT
        return lpkt

    def clean_header(self, pkt):
        header = pkt[0]
        header["fragment"] = str(0)
        header["e"] = LAST_PKT
        return pkt

    def is_last_fragment(self, pkt):
        header = pkt[0]
        if int(header["e"]) == LAST_PKT:
            return True
        else:
            return False

    def is_final(self, pkt):
        if pkt[0]["stop_func"] == self.__class__.__name__:
            return True
        return False

    def move_data_file(self, src, dst, postfix=None):

        PmkShared._ensure_dir(self.context.getWorkingDir()+dst)
        if postfix:
            dst_file = self.context.getWorkingDir()+"/"+dst+"/"+src+"-"+postfix
        else:
            dst_file = self.context.getWorkingDir()+"/"+dst+"/"+src
        dst_file = dst_file.replace("//","/")
        src_file = self.context.getWorkingDir()+src

        shutil.move(src_file,dst_file)
        return dst_file

    def is_fragment(self,pkt):
        header = pkt[0]
        if(int(header["fragment"]) > 0):
            return True
        return False

    def get_fragment_id(self, pkt):
        header = pkt[0]
        return int(header["fragment"])

    def get_pkt_data(self, pkt):
        l = len(pkt)
        data = pkt[l-2]["data"]
        return data

    def set_pkt_data(self,pkt,data):
        l = len(pkt)
        pkt[l-2]["data"] = data
        return pkt

    def merge_pkt(self, pkt):
        dpkt = pkt#copy.deepcopy(pkt)
        dpkt[0]["state"] = "MERGE"
        pkt_id = self.get_pkt_id(dpkt)
        #self._lock_in_fpkts.acquire()
        #self.context.pktReady(dpkt)
        #if pkt_id in self.in_flight_pkts: del self.in_flight_pkts[pkt_id]
        #self._lock_in_fpkts.release()
        if not pkt[0]["last_func"] == None:
             exdisp = self.context.getExternalDispatch()
             exdisp.send_to_last(dpkt)
        pass

    def track(self, data):
        mx = self.context.get_mx()
        mx.put(data)

    def dispatch(self, dpkt, msg, tag, type=None, fragment = False, dispatch = True):

        self._pkt_end_timing(dpkt)

        pkt = dpkt
        lpkt = dpkt
        caller = "run"
        aux = 0
        if "aux" in lpkt[0].keys():
            aux = lpkt[0]["aux"]

        if aux & Packet.TRACK_BIT:
            d = {}
            d["ship_id"] = self.get_ship_id(lpkt)
            d["stag"] = tag
            d["host"] = last_entry = lpkt[len(lpkt)-1]["ep"]
            d["data"] = msg
            self.track(d)

        if not self.context.is_speedy():
            pkt = copy.deepcopy(dpkt)
            caller = inspect.stack()[1][3]
            #logging.debug("Caller for dispatch function: "+inspect.stack()[1][3])
            if str(msg).startswith("file://") and not self.is_final(pkt):
                dst = self.context.getFileDir()
                _,path,file,src,_ = self.fileparts(msg)

                if not os.path.isfile(dst+file):
                    shutil.move(src,dst)
                else:
                    logging.warn("Trying to overwrite: "+str(dst+file))
                #msg = "tftp://"+self.context.get_local_ip()+"/"+file
                msg = self.context.getFileServerEndPoint()+"/"+file



            if self.is_fragment(pkt) and caller == "run":
                if not type:
                     otype = self.conf["return"][0]["type"]
                else:
                     otype = type
                stag = otype + ":"  + tag
                #Add output of current function
                lpkt = pkt
                last_entry = lpkt[len(lpkt)-1]
                pkt_e = {}
                pkt_e["stag"] = stag
                pkt_e["func"] = self.__class__.__name__+"."+caller
                pkt_e["exstate"] = "0001"
                pkt_e["data"] = msg
                pkt_e["ep"] = last_entry["ep"]
                lpkt.insert(len(lpkt)-1,pkt_e)

                self.merge_pkt(pkt)
                return

            if fragment:
                lpkt = copy.deepcopy(pkt)
                header = lpkt[0]
                fragment = int(header["fragment"])
                fragment = fragment + 1
                header["fragment"] = str(fragment)
                pkt[0]["fragment"] = str(fragment)
            else:
                lpkt = copy.copy(pkt)

        lpkt_id = self.get_pkt_id(lpkt)
        if not type:
            otype = self.conf["return"][0]["type"]
        else:
            otype = type

        stag = self.get_group()+":"+otype + ":"  + tag

        tstag = "OUT:"+self.__class__.__name__+":"+stag
        self.inc_state_counter(tstag)

        #Add output of current function
        last_entry = lpkt[len(lpkt)-1]
        pkt_e = {}
        pkt_e["stag"] = stag
        pkt_e["func"] = self.__class__.__name__+"."+caller
        pkt_e["exstate"] = "0001"
        pkt_e["data"] = msg
        pkt_e["ep"] = last_entry["ep"]
        lpkt[len(lpkt)-1] = pkt_e
        #lpkt.append(pkt_e)



        if self.context.with_acks():
            lpkt[0]["state"] = "WAITING_PACK"
        else:
            lpkt[0]["state"] = "TRANSIT"
        lpkt[0]["c_tag"] = stag

        #lpkt[0]["ttl"] = '60'
        lpkt[0]["t_state"] = tag
        lpkt[0]["t_otype"] = otype
        lpkt[0]["last_func"] = self.__class__.__name__

        if lpkt[0]["stop_func"] == self.__class__.__name__:
            if self.context.with_acks():
                lpkt[0]["state"] == "PACK_OK"
            else:
                lpkt[0]["state"] == "DONE"
            logging.debug("Stop function reached ["+lpkt[0]["stop_func"]+"]")
            if self.context.with_acks():
                self.ack_pkt(lpkt)
            return

        if dispatch:
            if self.context.with_acks():
                self.add_flight_pkt(lpkt)

            if self.context.with_shelve():
                pkt_id = self.get_pkt_id(lpkt)
                self._lock_in_fpkts.acquire()
                shelve = self.context.get_pkt_shelve()
                shelve[str(pkt_id)] = lpkt
                self._lock_in_fpkts.release()

            #Eats up memory due to queueing
            #self.context.getTx().put((self.get_group(), tag,otype,lpkt))
            #txx = self.context.getTx()
            #logging.debug("Tx Queue: "+str(txx.maxsize))
            self.context.getTx().put((self.get_group(), tag,otype,lpkt), True)

        return lpkt

    def re_dispatch(self,pkt):

        self._pkt_reset_timing(pkt)

        pkt_l = len(pkt)
        pkt[0]["state"] = "REDISPATCH"
        last = pkt[pkt_l - 1]
        stag = last["stag"]
        stag_spl = stag.split(":")
        if "traces" not in last.keys():
            traces = {}
            entry = {}
            last["traces"] = traces

            traces[last["ep"]] = entry
            entry["ep"] = last["ep"]
            entry["load"] = self._forecast["npkts"]
            entry["tries"] = 1
        else:
            traces = last["traces"]
            if last["ep"] not in last["traces"].keys():
                entry = {}
                traces[last["ep"]] = entry
                entry["ep"] = last["ep"]
                entry["load"] = self._forecast["npkts"]
                entry["tries"] = 1
            else:
                entry = traces[last["ep"]]
                entry["load"] = self._forecast["npkts"]
                entry["tries"] += 1

        #print json.dumps(pkt)
        #pkt.pop()
        #print json.dumps(pkt)
        self.context.getTx().put((stag_spl[0], stag_spl[2],stag_spl[1], pkt), True)
        return pkt


    def error(self,pkt, msg=None, type="ERROR", tag="ERROR"):

        self._pkt_reset_timing(pkt)

        lpkt = pkt
        caller = "run"
        stag = "ERROR"
        lpkt_id = self.get_pkt_id(pkt)
        if not type:
            otype = self.conf["return"][0]["type"]
        else:
            otype = type
        stag = self.get_group()+":"+otype + ":"  + tag

        tstag = "OUT:"+self.__class__.__name__+":"+stag
        self.inc_state_counter(tstag)

        #Add output of current function
        last_entry = lpkt[len(lpkt)-1]
        pkt_e = {}
        pkt_e["stag"] = stag
        pkt_e["func"] = self.__class__.__name__+"."+caller
        pkt_e["exstate"] = "0001"
        pkt_e["data"] = msg
        pkt_e["ep"] = last_entry["ep"]
        lpkt[len(lpkt)-1] = pkt_e


        lpkt[0]["state"] = "ERROR"
        lpkt[0]["c_tag"] = stag
        #lpkt[0]["ttl"] = '60'
        lpkt[0]["t_state"] = tag
        lpkt[0]["t_otype"] = otype
        lpkt[0]["last_func"] = self.__class__.__name__


        if self.context.with_shelve():
            pkt_id = self.get_pkt_id(lpkt)
            self._lock_in_fpkts.acquire()
            shelve = self.context.get_pkt_shelve()
            shelve[str(pkt_id)] = pkt
            self._lock_in_fpkts.release()


        pass



    def finalize(self,pkt, msg=None, type="END", tag="END"):

        self._pkt_end_timing(pkt)

        lpkt = pkt
        caller = "run"
        stag = "END"
        lpkt_id = self.get_pkt_id(pkt)

        if "aux" in lpkt[0].keys():
            aux = lpkt[0]["aux"]

        if aux & Packet.TRACK_BIT:
            d = {}
            d["ship_id"] = self.get_ship_id(lpkt)
            d["stag"] = tag
            d["host"] = last_entry = lpkt[len(lpkt)-1]["ep"]
            self.track(d)

        if not type:
            otype = self.conf["return"][0]["type"]
        else:
            otype = type
        stag = self.get_group()+":"+otype + ":"  + tag

        tstag = "OUT:"+self.__class__.__name__+":"+stag
        self.inc_state_counter(tstag)

        #Add output of current function
        last_entry = lpkt[len(lpkt)-1]
        pkt_e = {}
        pkt_e["stag"] = stag
        pkt_e["func"] = self.__class__.__name__+"."+caller
        pkt_e["exstate"] = "0001"
        pkt_e["data"] = msg
        pkt_e["ep"] = last_entry["ep"]
        lpkt[len(lpkt)-1] = pkt_e


        lpkt[0]["state"] = "DONE"
        lpkt[0]["c_tag"] = stag
        #lpkt[0]["ttl"] = '60'
        lpkt[0]["t_state"] = tag
        lpkt[0]["t_otype"] = otype
        lpkt[0]["last_func"] = self.__class__.__name__


        if self.context.with_shelve():
            pkt_id = self.get_pkt_id(lpkt)
            self._lock_in_fpkts.acquire()
            shelve = self.context.get_pkt_shelve()
            shelve[str(pkt_id)] = pkt
            self._lock_in_fpkts.release()


        pass



    def add_flight_pkt(self,pkt):
        """
            Add packet to ACK buffer.
        """
        pkt_id = self.get_pkt_id(pkt)
        self._lock_fpkts.acquire()
        self.flight_pkts[pkt_id] = pkt
        self._lock_fpkts.release()

        pass

    def get_name(self):
        return self.__class__.__name__

    def get_fullname(self):
        fname = self.get_group()+":"+self.__class__.__name__
        return fname


    def on_load(self):
        logging.warn("Class \""+self.__class__.__name__+"\" called on_load but not implimented.")
        pass


    def on_unload(self):
        logging.warn("Class \""+self.__class__.__name__+"\" called on_unload but not implimented.")
        pass

    def pre_run(self, pkt, *args):
        return True