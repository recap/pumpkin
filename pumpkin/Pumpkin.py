__author__ = 'reggie'

import uuid
import re
import imp
import signal

from os import listdir
from os.path import isfile, join
from socket import *
from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent



from PmkShared import *
from PmkContexts import *
from PmkBroadcast import *
from PmkSeed import *
from PmkShell import *
from PmkHTTPServer import *
#from PmkPeers import *
#from PmkPackets import *
#from PmkExternalDispatch import *
#from PmkInternalDispatch import *
#from PmkHTTPServer import *
#from PmkShell import *

class Pumpkin(object):
    def __init__(self):
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





    def startContext(self):
        context = self.context
        zmq_context = self.zmq_context

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

        if not context.isWithNoPlugins() and not context.isSupernode():

            for sn in get_zmq_supernodes(SUPERNODES):
                log.debug("Subscribing to: "+sn)
                zmqsub = ZMQBroadcastSubscriber(context, zmq_context, sn)
                zmqsub.start()
                context.addThread(zmqsub)

            onlyfiles = [ f for f in listdir(context.getTaskDir()) if isfile(join(context.getTaskDir(),f)) ]
            for fl in onlyfiles:
                fullpath = context.getTaskDir()+"/"+fl
                modname = fl[:-3]
                #ext = fl[-2:]

                if( fl[-2:] == "py"):
                    log.debug("Found module: "+fullpath)
                    file_header = ""
                    #try:
                    imp.load_source(modname,fullpath)

                    fh = open(fullpath, "r")
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
                            #print klass.getparameters()
                            #print klass.getreturn()


                    #except Exception:
                    #    log.error("Import error "+ str(Exception))



            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               js = klass.getConfEntry()
               log.debug(js)
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

            tcpm = ZMQPacketMonitor(context, zmq_context, "ipc:///tmp/"+context.getUuid())
            tcpm.start()
            context.addThread(tcpm)

            if context.hasShell():
                cmdp = Shell(context)
                cmdp.cmdloop()
