__author__ = 'reggie'

import inspect
import uuid
import copy
import tarfile
import shutil
import tftpy

from PmkShared import *

plugins = []
hplugins = {}
iplugins = {}

PKT_NEWBOX = 01
PKT_OLDBOX = 02

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


        pass

    def __rawpacket(self):
        pkt = []
        ship_id = self.context.getExecContext()
        cont_id =  str(uuid.uuid4())[:8]
        pkt.append({"ship" : ship_id, "container" : cont_id, "box" : '0', "e" : "E"})
        pkt.append( {"stag" : "RAW", "exstate" : "0001"} )
        return pkt

    def rawrun(self):
        self.run(self.__rawpacket())
        pass

    def _tar_to_gz(self, source, destination):
        t = tarfile.open(name=destination, mode='w:gz')
        t.add(source, os.path.basename(source))
        t.close()
        pass

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

    def _stage_run(self,pkt, *args):
        nargs = []
        if(args[0]):
            msg = str(args[0])
            if(msg.startswith("tftp://")):
                ip, port, rpath, file = self.__endpoint_parts(msg)
                client = tftpy.TftpClient(ip, int(port))
                wdf = self.context.getWorkingDir()+"/"+file
                client.download(file, wdf)
                nargs.append("file://"+wdf)
        for x in range (1,len(args)-1):
            nargs.append(args[x])

        self.run(pkt,*nargs)
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

    def dispatch(self, pkt, msg, state, boxing = PKT_OLDBOX):

        if str(msg).startswith("file://"):
            dst = self.context.getFileDir()
            _,path,file,src = self.fileparts(msg)

            shutil.move(src,dst)
            #msg = "tftp://"+self.context.getLocalIP()+"/"+file
            msg = self.context.getFileServerEndPoint()+"/"+file

        if boxing == PKT_NEWBOX:
            lpkt = copy.deepcopy(pkt)
            header = lpkt[0]
            box = int(header["box"])
            box = box + 1
            header["box"] = str(box)
            pkt[0]["box"] = str(box)
        else:
            lpkt = copy.copy(pkt)

        otype = self.conf["return"][0]["type"]
        stag = otype + ":"  + state
        pkt_e = {}
        pkt_e["stag"] = stag
        pkt_e["func"] = self.__class__.__name__
        pkt_e["exstate"] = "0001"
        pkt_e["data"] = msg
        lpkt.append(pkt_e)
        self.context.getTx().put((state,otype,lpkt))
        pass

    def getname(self):
        return self.__class__.__name__


    def on_load(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_load but not implimented.")
        pass


    def on_unload(self):
        log.warn("Class \""+self.__class__.__name__+"\" called on_unload but not implimented.")
        pass