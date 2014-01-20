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


from PmkShared import *
from PmkExternalDispatch import ExternalDispatch


plugins = []
hplugins = {}
iplugins = {}

PKT_NEWBOX = 01
PKT_OLDBOX = 02
PKT_PROCESS_WINDOW = 2

class SeedType(type):
    def __init__(cls, name, bases, attrs):
        super(SeedType, cls).__init__(name, bases, attrs)

        #print(cls, name, cls.__module__)
        if name != "Seed":
            plugins.append(cls)
            #log.debug("Adding: "+name)
            hplugins[name] = cls


class Seed(object):
    __metaclass__ = SeedType



    def __init__(self, context, poi="Unset"):
        self.context = context
        self.poi = poi
        self.conf = None
        self.tftp_sessions = {}
        self.flight_pkts = {}
        self._lock_fpkts = threading.Lock()
        self._lock_in_fpkts = threading.Lock()
        self.in_flight_pkts = {}
        self.pkt_checker_t()
        self.ttf = None
        #threading.Timer(10, self.pkt_checker_t).start()
        #try:
        #thread.start_new_thread(self.pkt_checker_t)
        #except:
         #   log.error("Unable to start pkt checker thread.")


        pass

    def getName(self):
        return self.__class__.__name__

    def __rawpacket(self):
        pkt = []
        ship_id = self.context.getExecContext()
        cont_id =  str(uuid.uuid4())[:8]
        pkt.append({"ship" : ship_id, "container" : cont_id, "box" : '0', "fragment" : '0', "e" : "E", "state" : "NEW", "ttl" : '0',"t_state":"None", "t_otype" : "None" , "last_contact" : "None", "last_func" : "None"})
        pkt.append( {"stag" : "RAW", "exstate" : "0001"} )
        return pkt

    def getNewContainer(self):
        return str(uuid.uuid4())[:8]

    def rawrun(self):
        self.run(self.__rawpacket())
        pass

    def getContId(self,pkt):
        return pkt[0]["container"]

    def getShipId(self, pkt):
        return pkt[0]["ship"]

    def _tar_to_gz(self, source, destination=None, suffix="test"):
        src = self.context.getWorkingDir()+source
        if destination == None:
            destination = self.context.getWorkingDir()+source+"-"+suffix+".tar.gz"

        t = tarfile.open(name=destination, mode='w:gz')
        t.add(src, os.path.basename(src))
        t.close()

        return destination


    def _untar_to_wd(self, source, destination=None):
        fp = source

        if not os.path.isfile(fp):
            fp = self.context.getWorkingDir() +"/"+ source
            if os.path.isfile(fp):
                if not destination:
                    destination = self.context.getWorkingDir()
                t = tarfile.open(fp)
                t.extractall(destination)
                t.close()
                return fp
            else:
                log.error("Input file not found ["+source+"]")


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
        dpkt = copy.copy(pkt)
        dpkt[0]["state"] = "PACK_OK"
        pkt_id = self.getPktId(dpkt)
        self._lock_in_fpkts.acquire()
        self.context.pktReady(dpkt)
        if pkt_id in self.in_flight_pkts: del self.in_flight_pkts[pkt_id]
        self._lock_in_fpkts.release()
        if not pkt[0]["last_func"] == None:
            exdisp = self.context.getExternalDispatch()
            exdisp.sendPACK(dpkt)

        pass


    def pack_ok(self, pkt):
        pkt_id = self.getPktId(pkt)
        if pkt[0]["last_func"] == self.__class__.__name__:
            self._lock_fpkts.acquire()
            if pkt_id in self.flight_pkts.keys():
                del self.flight_pkts[pkt_id]
            self._lock_fpkts.release()
        else:
            log.warning("Trying to ACK packet from another function!")
        pass

    def isDuplicate(self, pkt):
        pkt_id = self.getPktId(pkt)
        self._lock_in_fpkts.acquire()

        if pkt_id in self.in_flight_pkts.keys():
            self._lock_in_fpkts.release()
            return True
        if self.context.isPktShelved(pkt):
            self._lock_in_fpkts.release()
            return True
        self._lock_in_fpkts.release()
        return False

    def _stage_run(self,pkt, *args):
        pkt_id = self.getPktId(pkt)
        if not self.isDuplicate(pkt):


            pkt[0]["state"] = "PROCESSING"
            self._lock_in_fpkts.acquire()
            self.in_flight_pkts[pkt_id] = pkt
            self._lock_in_fpkts.release()
            try:
                nargs = []
                if(args[0]):
                    msg = str(args[0])
                    if(msg.startswith("tftp://")):
                        ip, port, rpath, file = self.__endpoint_parts(msg)
                        if ip in self.tftp_sessions.keys():
                            client = self.tftp_sessions[ip]
                        else:
                            client = tftpy.TftpClient(ip, int(port))
                            self.tftp_sessions[ip] = client

                        wdf = self.context.getWorkingDir()+"/"+file
                        client.download(file, wdf)
                        nargs.append("file://"+wdf)
                        for x in range (1,len(args)-1):
                            nargs.append(args[x])

                        self.run(pkt,*nargs)
                        return

                self.run(pkt,*args)
            except Exception as e:
                log.error(str(e))
                self._lock_in_fpkts.acquire()

                if pkt_id in self.in_flight_pkts.keys():
                    del self.in_flight_pkts[pkt_id]

                self._lock_in_fpkts.release()
                pass
        else:
            log.debug("Duplicate packet received: "+pkt_id)
            pass

    def run(self, pkt, *args):
        pass

    def getpoi(self):
        return self.poi

    def setconf(self, jconf):
        self.conf = jconf
        pass

    def hasInputs(self):
        if len(self.conf["parameters"]) > 0:
            return True

        return False

    def pkt_checker_t(self):
        log.debug("Starting pkt checker thread")
        #time.sleep(10)

        #while 1:
        interval = 30
        reset = 60
        self._lock_fpkts.acquire()

        for k in self.flight_pkts.keys():
            pkt = self.flight_pkts[k]
            ttl = int(pkt[0]["ttl"])
            ttl = ttl - interval
            if ttl <= 0:
                ttl = reset
                log.debug("Resending packet: "+str(pkt))
                state = pkt[0]["t_state"]
                otype = pkt[0]["t_otype"]
                self.context.getTx().put((state, otype, pkt))
                pass
            pkt[0]["ttl"] = str(ttl)

        self._lock_fpkts.release()

        threading.Timer(interval, self.pkt_checker_t).start()

         #   time.sleep(10)

        pass

    def getConfEntry(self):
        js = '{ "name" : "'+self.getname()+'", \
       "endpoints" : [ '+self.__getEps()+' ],' \
       ''+self.getparameters()+',' \
       ''+self.getreturn()+'}'

       # js = '{ "name" : "'+self.getname()+'", \
       #"zmq_endpoint" : [ {"ep" : "'+self.context.endpoints[0]+'", "cuid" : "'+self.context.getUuid()+'"} ],' \
       #''+self.getparameters()+',' \
       #''+self.getreturn()+'}'
        return js

    def _ensure_dir(self, f):
        if not f[len(f)-1] == "/":
                f = f +"/"
        d = os.path.dirname(f)
        if not os.path.exists(d):
            log.debug(d + " does not exist, creating...")
            os.makedirs(d)
        pass

    def __getEps(self):
        aep = ""
        for ep in self.context.endpoints:
            s =  '{"ep" : "'+ep[0]+'", "cuid" : "'+self.context.getUuid()+'", "type" : "'+ep[1]+'", "mode" : "'+ep[2]+'", "priority" : "'+str(ep[3])+'"}'
            aep = aep + s + ","
        aep = aep[:len(aep)-1]
        return aep


    def getparameters(self):
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
        self.dispatch(pkt, msg, state, PKT_NEWBOX)
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


        file = prts2[len(prts2)-1]
        path = path.replace("//","/")
        apath = path+file

        return [prot,path,file,apath]

    def getPktId(self, pkt):
        id= pkt[0]["ship"]+":"+pkt[0]["container"]+":"+pkt[0]["box"]+":"+pkt[0]["fragment"]
        return id

    def duplicate_pkt_new_box(self,pkt):
        lpkt = copy.deepcopy(pkt)
        cont = self.getNewContainer()
        header = lpkt[0]
        #box = int(header["box"])
        #box = box + 1
        header["container"] = cont
        return lpkt

    def dispatch(self, dpkt, msg, state, boxing = PKT_OLDBOX):


        pkt = copy.deepcopy(dpkt)

        if str(msg).startswith("file://"):
            dst = self.context.getFileDir()
            _,path,file,src = self.fileparts(msg)

            shutil.move(src,dst)
            #msg = "tftp://"+self.context.getLocalIP()+"/"+file
            msg = self.context.getFileServerEndPoint()+"/"+file

        if boxing == PKT_NEWBOX:
            lpkt = copy.deepcopy(pkt)
            header = lpkt[0]
            fragment = int(header["fragment"])
            fragment = fragment + 1
            header["fragment"] = str(fragment)
            pkt[0]["fragment"] = str(fragment)
        else:
            lpkt = copy.copy(pkt)

        lpkt_id = self.getPktId(lpkt)
        otype = self.conf["return"][0]["type"]
        stag = otype + ":"  + state
        pkt_e = {}
        pkt_e["stag"] = stag
        pkt_e["func"] = self.__class__.__name__
        pkt_e["exstate"] = "0001"
        pkt_e["data"] = msg
        lpkt.append(pkt_e)
        lpkt[0]["state"] = "WAITING_PACK"
        lpkt[0]["ttl"] = '60'
        lpkt[0]["t_state"] = state
        lpkt[0]["t_otype"] = otype
        lpkt[0]["last_func"] = self.__class__.__name__

        if lpkt[0]["stop_func"] == self.__class__.__name__:
            lpkt[0]["state"] == "PACK_OK"
            log.debug("Stop function reached ["+lpkt[0]["stop_func"]+"]")
            self.ack_pkt(lpkt)
            return

        self.addFlightPacket(lpkt)


        self.context.getTx().put((state,otype,lpkt))
        pass
    def addFlightPacket(self,pkt):
        pkt_id = self.getPktId(pkt)
        while 1:
            if len(self.flight_pkts) < PKT_PROCESS_WINDOW:
                self._lock_fpkts.acquire()
                self.flight_pkts[pkt_id] = pkt
                self._lock_fpkts.release()
                break
            else:
                log.debug("Process window full for "+self.__class__.__name__)
                time.sleep(5)
        pass

    def getname(self):
        return self.__class__.__name__


    def on_load(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_load but not implimented.")
        pass


    def on_unload(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_unload but not implimented.")
        pass