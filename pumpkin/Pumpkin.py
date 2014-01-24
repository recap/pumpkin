__author__ = 'reggie'

import uuid
import re
import imp
import signal
import sys
import argparse
import logging
import shutil
import errno
import ConfigParser



import shelve, os, fcntl, new
import __builtin__
from fcntl import LOCK_SH, LOCK_EX, LOCK_UN, LOCK_NB



from os import listdir
from os.path import isfile, join
from socket import *
from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent


from os.path import expanduser




import pumpkin as pmk

import PmkShared

#from PmkShared import *
from PmkContexts import *
from PmkBroadcast import *
from PmkShell import *
from PmkHTTPServer import *
from PmkDaemon import *
from PmkDataCatch import *
from PmkTftpServer import *
from PmkFtpServer import *





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

        local_peers = self._shelve_safe_open("/tmp/pumpkin")
        if self.context.getUuid() in local_peers: del local_peers[self.context.getUuid()]
        local_peers.close()

        self.context.close()
        for th in self.context.getThreads():
            th.stop()
            #th.join()
        time.sleep(2)
        log.info("Exiting Pumpkin")
        pass

    def run(self):
        self.startContext()
        pass

    def _shelve_safe_close(self, selv):
        shelve.Shelf.close(selv)
        fcntl.flock(selv.lckfile.fileno(), LOCK_UN)
        selv.lckfile.close()

    def _shelve_safe_open(self, filename, flag='c', protocol=None, writeback=False, block=True, lckfilename=None):
        """Open the sheve file, createing a lockfile at filename.lck.  If
        block is False then a IOError will be raised if the lock cannot
        be acquired"""
        if lckfilename == None:
            lckfilename = filename + ".lck"
        lckfile = __builtin__.open(lckfilename, 'w')

        # Accquire the lock
        if flag == 'r':
            lockflags = LOCK_SH
        else:
            lockflags = LOCK_EX
        if not block:
            lockflags = LOCK_NB
        fcntl.flock(lckfile.fileno(), lockflags)

        # Open the shelf
        shelf = shelve.open(filename, flag, protocol, writeback)

        # Override close
        shelf.close = new.instancemethod(self._shelve_safe_close, shelf, shelve.Shelf)
        shelf.lckfile = lckfile

        # And return it
        return shelf

    def _checkLocalPeers(self, zmq_context):
        try:
            local_peers = self._shelve_safe_open("/tmp/pumpkin")

            for p in local_peers:
                if p not in self.context.peers:
                    self.context.peers[p] = local_peers[p]
                    log.debug("Subscribing to new Peer ["+local_peers[p]+"]")
                    zmqsub = ZMQBroadcastSubscriber(self.context, zmq_context, local_peers[p])
                    zmqsub.start()
                    self.context.addThread(zmqsub)

            local_peers.close()
        except Exception as er:
            log.error(str(er))
            local_peers.close()

        threading.Timer(10, self._checkLocalPeers, [zmq_context]).start()

        pass

    def startContext(self):
        context = self.context
        log.info("Node assigned UID: "+context.getUuid())
        log.info("Exec context: "+context.getExecContext())
        log.info("Node bound to IP: "+context.getLocalIP())
        home = expanduser("~")
        wd = home+"/.pumpkin/"+context.getUuid()+"/"
        context.working_dir = wd

        PmkShared._ensure_dir(wd)
        context.startPktShelve("PktStore")
        context.peers[context.getUuid()] = "/tmp/"+context.getUuid()+"-bcast"
        local_peers = self._shelve_safe_open("/tmp/pumpkin")
        local_peers[context.getUuid()] = "ipc:///tmp/"+context.getUuid()+"-bcast"
        local_peers.close()



        log.debug("Working directory: "+context.getWorkingDir())

        #context.openfiles.append(context.getWorkingDir())

        context = self.context
        zmq_context = self.zmq_context
        context.zmq_context = zmq_context
        #PmkShared.ZMQ_PUB_PORT  = PmkShared._get_nextport(ZMQ_PUB_PORT, "TCP")
        PmkShared.ZMQ_ENDPOINT_PORT = PmkShared._get_nextport(ZMQ_ENDPOINT_PORT, "TCP")
        PmkShared.TFTP_FILE_SERVER_PORT  = PmkShared._get_nextport(TFTP_FILE_SERVER_PORT, "UDP")

        context.setEndpoints()


        udplisten = BroadcastListener(context, int(context.getAttributeValue().bcport))
        udplisten.start()
        context.addThread(udplisten)

        #Local bus
        zmqbc = ZMQBroadcaster(context, zmq_context, "ipc:///tmp/"+context.getUuid()+"-bcast")
        context.openfiles.append("/tmp/"+context.getUuid()+"-bcast")
        #context.openfiles.append("/tmp/pumpkin-bus")
        zmqbc.start()
        context.addThread(zmqbc)

        # zmqsub = ZMQBroadcastSubscriber(context, zmq_context, "ipc:///tmp/"+context.getUuid()+"-bus")
        #zmqsub = ZMQBroadcastSubscriber(context, zmq_context, "ipc:///tmp/pumpkin-bus")
        #zmqsub.start()
        #context.addThread(zmqsub)


        ftpdir = wd + 'fdata/'

        tftpserver = TftpServer(context, ftpdir, PmkShared.TFTP_FILE_SERVER_PORT)
        tftpserver.start()
        context.setFileDir(ftpdir)
        context.addThread(tftpserver)

        ftpserver = FtpServer(context, context.getWorkingDir())
        ftpserver.start()
        context.addThread(ftpserver)




        if context.hasRx():
            rxdir = context.hasRx()
            pfm = PacketFileMonitor(context, rxdir)
            pfm.start()
            context.addThread(pfm)
        else:
            rxdir = context.getWorkingDir()+"/rx"
            pfm = PacketFileMonitor(context, rxdir)
            pfm.start()
            context.addThread(pfm)


        if context.isSupernode():
            log.debug("In supernode mode")
            #udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
            #udplisten.start()
            #context.addThread(udplisten)

            zmqbc = ZMQBroadcaster(context, zmq_context, "tcp://*:"+str(PmkShared.ZMQ_PUB_PORT))
            zmqbc.start()
            context.addThread(zmqbc)

            http = HttpServer(context)
            http.start()
            context.addThread(http)


        if not context.isWithNoPlugins():# and not context.isSupernode():
            for ep in context.getEndpoints():
                if context.isZMQEndpoint(ep):
                    #ep[0] = PmkShared._get_nextport(ep[0], "TCP")
                    tcpm = ZMQPacketMonitor(context, zmq_context, ep[0])
                    tcpm.start()
                    context.addThread(tcpm)


            for sn in get_zmq_supernodes(PmkShared.SUPERNODES):
                log.debug("Subscribing to: "+sn)
                zmqsub = ZMQBroadcastSubscriber(context, zmq_context, sn)
                zmqsub.start()
                context.addThread(zmqsub)
                pass

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
                                        klass.set_conf(d)

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
                                klass.set_conf(d)

            except Exception as e:
                log.error("Import error "+ str(e))
                pass

            for fd in listdir(context.getTaskDir()):
                src = context.getTaskDir()+"/"+fd
                dst = context.getWorkingDir()+"/"+fd
                try:
                    shutil.copytree(src, dst)
                except OSError as exc: # python >2.5
                    if exc.errno == errno.ENOTDIR:
                        shutil.copy(src, dst)
                    else: raise


            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               js = klass.getConfEntry()
               #log.debug(js)
               context.getProcGraph().updateRegistry(json.loads(js), loc="locallocal")
               #context.getProcGraph().updateRegistry(json.loads(js), loc="locallocal")
               #context.getProcGraph().updateRegistry(json.loads(js), loc="locallocal")

            log.debug("Registry dump: "+context.getProcGraph().dumpExternalRegistry())

            udpbc = Broadcaster(context, int(context.getAttributeValue().bcport))
            udpbc.start()

            context.addThread(udpbc)

            edispatch = ExternalDispatch(context)
            context.setExternalDispatch(edispatch)
            edispatch.start()
            context.addThread(edispatch)

            idispatch = InternalDispatch(context)
            idispatch.start()
            context.addThread(idispatch)

            #context.startDisplay()

            self._checkLocalPeers(zmq_context)

            ##############START TASKS WITH NO INPUTS#############################
            inj = Injector(context)
            inj.start()
            context.addThread(inj)
            #####################################################################





            if context.hasShell():
                cmdp = Shell(context)
                cmdp.cmdloop()



from BaseHTTPServer import HTTPServer
from SocketServer import ThreadingMixIn

#class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
#    """Handle requests in a separate thread."""



def main():

    log = logging.getLogger("pumpkin")
    log.setLevel(logging.DEBUG)

    requests_log = logging.getLogger("tftpy")
    requests_log.setLevel(logging.WARNING)
    #log.setLevel(logging.INFO)


    ###################TEST###############################3



    ######################################################3


    parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
    parser.add_argument('--noplugins',action="store_true",
                       help='disable plugin hosting for this node.')
    #FIXME remove this after SC13
    #parser.add_argument('--nobroadcast', action='store', dest="nobroadcast", default=False,
    #                   help='disable broadcasting.')
    parser.add_argument('--bcport', action='store', dest="bcport", default=7700,
                       help='broadcast UDP port.')
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
    parser.add_argument('--endpoints', action='store', dest="eps", default="ALL",
                       help='endpoints separated with ";" e.x. zmq.TCP;zmq.INPROC;zmq.IPC;tftp')
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
    parser.add_argument('-c', action='store', dest="config", default="./pumpkin.cfg",
                       help='config file')

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
        UDP_BROADCAST_PORT = int(context.getAttributeValue().bcport)
        config = ConfigParser.RawConfigParser(allow_no_value=True)
        if os.path.exists(args.config):
            config.read(args.config)
            PmkShared.SUPERNODES = config.get("supernodes", "hosts").split(",")
            x = 10
            pass


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
