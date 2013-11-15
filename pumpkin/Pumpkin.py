__author__ = 'reggie'

import uuid
import re
import imp
import signal
import sys

from os import listdir
from os.path import isfile, join
from socket import *
from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent



from PmkShared import *
from PmkContexts import *
from PmkBroadcast import *
from PmkShell import *
from PmkHTTPServer import *
from PmkDaemon import *
from PmkDataCatch import *

class Pumpkin(Daemon):
    def __init__(self, pidfile="/tmp/pumpkin.pid"):
        Daemon.__init__(self,pidfile, "/dev/null", "/tmp/pumpkin.stdout", "/tmp/pumpkin.stderr")
        uid = str(gethostname())+"-"+str(uuid.uuid4())[:8]
        ex_cntx = str(uuid.uuid4())[:8]

        #Create a context
        self.context = MainContext(uid)
        self.context.setExecContext(ex_cntx)
        self.context.setSupernodeList(SUPERNODES)
        self.context.setLocalIP(get_lan_ip())

        self.zmq_context = zmq.Context()
        pass


    def getContext(self):
        return self.context

    def stopContext(self):
        for th in self.context.getThreads():
            th.stop()
            #th.join()
        time.sleep(2)
        log.info("Exiting Pumpkin")
        pass

    def run(self):
        self.startContext()
        pass

    def startContext(self):
        context = self.context
        log.info("Node assigned UID: "+context.getUuid())
        log.info("Exec context: "+context.getExecContext())
        log.info("Node bound to IP: "+context.getLocalIP())
        log.debug("Working directory: "+str(os.getcwd()))

        context = self.context
        zmq_context = self.zmq_context

        if context.hasRx():
            rxdir = context.hasRx()
            pfm = PacketFileMonitor(context, rxdir)
            pfm.start()
            context.addThread(pfm)


        if context.isSupernode():
            log.debug("In supernode mode")
            udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
            udplisten.start()
            context.addThread(udplisten)

            zmqbc = ZMQBroadcaster(context, zmq_context, ZMQ_PUB_PORT)
            zmqbc.start()
            context.addThread(zmqbc)

            http = HttpServer(context)
            http.start()
            context.addThread(http)



        if not context.isWithNoPlugins():# and not context.isSupernode():

            for sn in get_zmq_supernodes(SUPERNODES):
                log.debug("Subscribing to: "+sn)
                zmqsub = ZMQBroadcastSubscriber(context, zmq_context, sn)
                zmqsub.start()
                context.addThread(zmqsub)
            try:
                if not context.singleSeed():
                    onlyfiles = [ f for f in listdir(context.getTaskDir()) if isfile(join(context.getTaskDir(),f)) ]
                    for fl in onlyfiles:
                        fullpath = context.getTaskDir()+"/"+fl
                        modname = fl[:-3]
                        #ext = fl[-2:]

                        if( fl[-2:] == "py"):
                            log.debug("Found seed: "+fullpath)
                            file_header = ""
                            fh = open(fullpath, "r")
                            fhd = fh.read()
                            m = re.search('##START-CONF(.+?)##END-CONF(.*)', fhd, re.S)

                            if m:
                                conf = m.group(1).replace("##","")
                                if conf:
                                    d = json.loads(conf)
                                    if not "auto-load" in d.keys() or d["auto-load"] == True:
                                        imp.load_source(modname,fullpath)

                                        klass = PmkSeed.hplugins[modname](context)
                                        PmkSeed.iplugins[modname] = klass
                                        klass.on_load()
                                        klass.setconf(d)

                else:
                    seedfp = context.singleSeed()
                    seedfpa = seedfp.split("/")
                    seedsp = seedfpa[len(seedfpa) -1 ]
                    modname = seedsp[:-3]
                    if( seedsp[-2:] == "py"):
                        log.debug("Found seed: "+seedfp)
                        file_header = ""
                        #try:
                        imp.load_source(modname,seedfp)

                        fh = open(seedfp, "r")
                        fhd = fh.read()
                        m = re.search('##START-CONF(.+?)##END-CONF(.*)', fhd, re.S)

                        if m:
                            conf = m.group(1).replace("##","")
                            if conf:
                                d = json.loads(conf)
                                klass = PmkSeed.hplugins[modname](context)
                                PmkSeed.iplugins[modname] = klass
                                klass.on_load()
                                klass.setconf(d)

            except Exception as e:
                log.error("Import error "+ str(e))
                pass



            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               js = klass.getConfEntry()
               #log.debug(js)
               context.getProcGraph().updateRegistry(json.loads(js))

            udpbc = Broadcaster(context)
            udpbc.start()

            context.addThread(udpbc)

            edispatch = ExternalDispatch(context)
            edispatch.start()
            context.addThread(edispatch)

            idispatch = InternalDispatch(context)
            idispatch.start()
            context.addThread(idispatch)

            #context.startDisplay()


            ##############START TASKS WITH NO INPUTS#############################
            inj = Injector(context)
            inj.start()
            context.addThread(inj)
            #####################################################################

            zmq_context = zmq.Context()


            tcpm = ZMQPacketMonitor(context, zmq_context, context.getEndpoint())
            tcpm.start()
            context.addThread(tcpm)

            if context.hasShell():
                cmdp = Shell(context)
                cmdp.cmdloop()
