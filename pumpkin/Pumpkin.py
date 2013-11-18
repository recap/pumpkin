__author__ = 'reggie'

import uuid
import re
import imp
import signal
import sys
import argparse
import logging


from os import listdir
from os.path import isfile, join
from socket import *
from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent


import pumpkin as pmk

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

        udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
        udplisten.start()
        context.addThread(udplisten)

        if context.hasRx():
            rxdir = context.hasRx()
            pfm = PacketFileMonitor(context, rxdir)
            pfm.start()
            context.addThread(pfm)


        if context.isSupernode():
            log.debug("In supernode mode")
            #udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
            #udplisten.start()
            #context.addThread(udplisten)

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


def main():
    log = logging.getLogger("root")
    log.setLevel(logging.DEBUG)
    #log.setLevel(logging.INFO)


    parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
    parser.add_argument('--noplugins',action="store_true",
                       help='disable plugin hosting for this node.')
    #FIXME remove this after SC13
    parser.add_argument('--nobroadcast', action='store', dest="nobroadcast", default=False,
                       help='disable broadcasting.')

    parser.add_argument('--broadcast',action="store_true",
                       help='broadcast on lan.')
    parser.add_argument('--taskdir', action='store', dest="taskdir", default="./examples/helloworld",
                       help='directory for loading tasks.')
    parser.add_argument('--rx', action='store', dest="rxdir", default=None,
                       help='directory for injecting data.')
    parser.add_argument('--seed', action='store', dest="singleseed", default=None,
                       help='load a single seed.')
    parser.add_argument('--supernode',action="store_true",
                       help='run in supernode i.e. main role is information proxy.')
    parser.add_argument('--endpoint.mode', action='store', dest="epmode", default="zmq.PULL",
                       help='endpoint mode e.x. zmq.PULL|zmq.PUB')
    parser.add_argument('--endpoint.type', action='store', dest="eptype", default="zmq.TCP",
                       help='endpoint type e.x. zmq.TCP|zmq.IPC|zmq.INPROC')
    parser.add_argument('--shell',action="store_true",
                       help='start a shell prompt.')
    parser.add_argument('--rest',action="store_true",
                       help='start rest interface for seeds.')
    parser.add_argument('--debug',action="store_true",
                       help='print debugging info.')
    parser.add_argument('-d', action='store', dest="daemon", default=None,
                       help='daemonize start|stop|restart')

    parser.add_argument('--version', action='version', version='%(prog)s '+pmk.VERSION)
    args = parser.parse_args()


    P = Pumpkin()

    if args.daemon == "start":
        log.info("Starting Pumpkin Daemon")
        context = P.getContext()
        context.setAttributes(args)
        P.start()
    if args.daemon == "stop":
        P.stop()
    if args.daemon == "restart":
        P.restart()
    if args.daemon == None:
        context = P.getContext()
        context.setAttributes(args)
        P.startContext()
        #Handle SIGINT
        def signal_handler(signal, frame):
                P.stopContext()


                log.info("Exiting Bye Bye")
                ##Ugly kill because threads zmq are not behaving
                os.system("kill -9 "+str(os.getpid()))

                sys.exit(0)


        #Catch Ctrl+C
        signal.signal(signal.SIGINT, signal_handler)
        signal.pause()
