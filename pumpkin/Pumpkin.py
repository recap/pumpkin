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
import cProfile, pstats
import pika
import multiprocessing




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

        #if os.path.isfile("/tmp/id")

        uid = str(gethostname())+"-"+str(uuid.uuid4())[:8]
        ex_cntx = str(uuid.uuid4())[:8]

        #Create a context
        self.context = MainContext(uid)
        self.context.setExecContext(ex_cntx)
        self.context.setSupernodeList(SUPERNODES)
        self.context.set_local_ip(get_lan_ip())
        self.context.set_public_ip(get_public_ip())
        self.context.__pumpkin = self

        self.zmq_context = zmq.Context()

        pass


    def getContext(self):
        return self.context

    def stopContext(self):

        local_peers = self._shelve_safe_open("/tmp/pumpkin.shelve")
        if self.context.getUuid() in local_peers: del local_peers[self.context.getUuid()]
        local_peers.close()

        self.context.close()
        for th in self.context.getThreads():
            th.stop()
            #th.join()
        time.sleep(2)
        logging.info("Exiting Pumpkin")
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
        local_peers = None
        try:
            local_peers = self._shelve_safe_open("/tmp/pumpkin.shelve")

            for p in local_peers:
                if p not in self.context.peers:
                    self.context.peers[p] = local_peers[p]
                    #logging.debug("Subscribing to new Peer ["+local_peers[p]+"]")
                    #zmqsub = ZMQBroadcastSubscriber(self.context, zmq_context, local_peers[p])
                    #zmqsub.start()
                    #self.context.addThread(zmqsub)

            local_peers.close()
        except Exception as er:
            logging.error(str(er))
            if local_peers:
                local_peers.close()

        threading.Timer(10, self._checkLocalPeers, [zmq_context]).start()

        pass

    def load_seed(self, file):
        context = self.context
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

                        klass = PmkSeed.hplugins[modname](context)
                        PmkSeed.iplugins[modname] = klass
                        klass.pre_load(d)
                        klass.on_load()
                        klass.post_load()


        return modname

    def startContext(self):
        context = self.context
        logging.info("Node assigned UID: "+context.getUuid())
        logging.info("Exec context: "+context.getExecContext())
        logging.info("Node bound to IP: "+context.get_local_ip())
        home = expanduser("~")
        wd = home+"/.pumpkin/"+context.getUuid()+"/"
        context.working_dir = wd

        PmkShared._ensure_dir(wd)
        PmkShared._ensure_dir(wd+"logs/")
        PmkShared._ensure_dir(wd+"seeds/")
        context.log_to_file()

        context.startPktShelve("PktStore")
        context.startPktShelve2("RemotePktStore")
        if context.is_speedy():
            logging.info("Running in speedy gonzales mode")

        logging.debug("Working directory: "+context.getWorkingDir())

        #context.openfiles.append(context.getWorkingDir())

        context = self.context
        zmq_context = self.zmq_context
        context.zmq_context = zmq_context
        #PmkShared.ZMQ_PUB_PORT  = PmkShared._get_nextport(ZMQ_PUB_PORT, "TCP")
        PmkShared.ZMQ_ENDPOINT_PORT = PmkShared._get_nextport(ZMQ_ENDPOINT_PORT, "TCP")
        logging.debug("ZMQ endpoint port: "+str(PmkShared.ZMQ_ENDPOINT_PORT))
        PmkShared.TFTP_FILE_SERVER_PORT  = PmkShared._get_nextport(TFTP_FILE_SERVER_PORT, "UDP")

        context.setEndpoints()

        if not context.is_ghost():
            #zmqbc = ZMQBroadcaster(context, zmq_context, "tcp://*:"+str(PmkShared.ZMQ_PUB_PORT))
            #zmqbc = ZMQBroadcaster(context, zmq_context, context.get_our_pub_ep("tcp"))
            #zmqbc.start()
            #context.addThread(zmqbc)
            pass

        if context.with_broadcast():
            #Listen for UDP broadcasts on LAN
            udplisten = BroadcastListener(context, int(context.getAttributeValue().bcport), zmq_context)
            udplisten.start()
            context.addThread(udplisten)


            udpbc = Broadcaster(context, int(context.getAttributeValue().bcport), rate = context.get_broadcast_rate())
            udpbc.start()
            context.addThread(udpbc)

            zmqbc = ZMQBroadcaster(context, zmq_context, "tcp://*:"+str(PmkShared.ZMQ_PUB_PORT))
            zmqbc = ZMQBroadcaster(context, zmq_context, context.get_our_pub_ep("tcp"))
            zmqbc.start()
            context.addThread(zmqbc)
            pass

        #Local stuff to exploit multi-cores still needs testing

        #context.peers[context.getUuid()] = "/tmp/"+context.getUuid()+"-bcast"
        #local_peers = self._shelve_safe_open("/tmp/pumpkin")
        #local_peers[context.getUuid()] = "ipc:///tmp/"+context.getUuid()+"-bcast"
        #local_peers.close()
        # zmqbc = ZMQBroadcaster(context, zmq_context, "ipc:///tmp/"+context.getUuid()+"-bcast")
        # context.openfiles.append("/tmp/"+context.getUuid()+"-bcast")
        # #context.openfiles.append("/tmp/pumpkin-bus")
        # zmqbc.start()
        # context.addThread(zmqbc)

        #zmqsub = ZMQBroadcastSubscriber(context, zmq_context, "ipc:///tmp/"+context.getUuid()+"-bus")
        #zmqsub = ZMQBroadcastSubscriber(context, zmq_context, "ipc:///tmp/pumpkin-bus")
        #zmqsub.start()
        #context.addThread(zmqsub)

        ftpdir = wd + 'tx/'

        if not context.is_ghost():
            #TFTP for sending files between pumpkins
            tftpserver = TftpServer(context, ftpdir, PmkShared.TFTP_FILE_SERVER_PORT)
            tftpserver.start()
            context.setFileDir(ftpdir)
            context.addThread(tftpserver)

            #FTP for user access of working directory
            ftpserver = FtpServer(context, context.getWorkingDir())
            ftpserver.start()
            context.addThread(ftpserver)

        edispatch = ExternalDispatch(context)
        context.setExternalDispatch(edispatch)
        edispatch.start()
        context.addThread(edispatch)

        if context.broadcast_rabbitmq():
            host, port, username, password, vhost = self.context.get_rabbitmq_cred()
            credentials = pika.PlainCredentials(username, password)

            connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,  credentials=credentials, virtual_host=vhost))
            rabbitmq = RabbitMQMonitor(context, connection)
            context.set_rabbitmq(rabbitmq)

            q = context.get_group()+":info"
            bunny = RabbitMQBroadcaster(context, exchange=q)
            bunny.start()
            context.addThread(bunny)

            bunnylistener = RabbitMQBroadcastSubscriber(context, exchange=q)
            bunnylistener.start()
            context.addThread(bunnylistener)

            logging.debug("Adding RabbitMQ queue: "+context.getUuid())
            rabbitmq.add_monitor_queue(context.getUuid())
            rabbitmq.add_monitor_queue(context.getUuid()+"ack")

            qm = context.get_group()+":track"
            monitor = RabbitMqLog(self.context)
            monitor.connect(qm)

            mon_dispatcher = LogDisptacher(self.context)
            mon_dispatcher.add_monitor(monitor)
            mon_dispatcher.start()
            context.addThread(mon_dispatcher)




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


        if context.isSupernode() and not context.is_ghost():
            logging.debug("In supernode mode")

            http = HttpServer(context)
            http.start()
            context.addThread(http)

        for fd in listdir(context.getTaskDir()):
                src = context.getTaskDir()+"/"+fd
                if( src[-3:] == "pyc"):
                    continue

                if( src[-2:] == "py"):
                    dst = context.getWorkingDir()+"/seeds/"+fd
                else:
                    dst = context.getWorkingDir()+"/"+fd
                try:
                    shutil.copytree(src, dst)
                except OSError as exc: # python >2.5
                    if exc.errno == errno.ENOTDIR:
                        shutil.copy(src, dst)
                    else: raise


        if not context.isWithNoPlugins():# and not context.isSupernode():
            for ep in context.getEndpoints():
                if context.isZMQEndpoint(ep):
                    #ep[0] = PmkShared._get_nextport(ep[0], "TCP")
                    tcpm = ZMQPacketMonitor(context, zmq_context, ep[0])
                    tcpm.start()
                    context.addThread(tcpm)


            for sn in get_zmq_supernodes(PmkShared.SUPERNODES):
                if not str(sn).__contains__("127.0.0.1"):
                    #logging.debug("Subscribing to: "+sn)
                    #zmqsub = ZMQBroadcastSubscriber(context, zmq_context, sn)
                    #zmqsub.start()
                    #context.addThread(zmqsub)
                    pass
                pass

            try:
                if not context.singleSeed():
                    onlyfiles = [ f for f in listdir(context.getTaskDir()) if isfile(join(context.getTaskDir(),f)) ]
                    for fl in onlyfiles:
                        fullpath = context.getTaskDir()+"/"+fl
                        modname = fl[:-3]
                        #ext = fl[-2:]
                        self.load_seed(fullpath)

                else:
                    seedfp = context.singleSeed()
                    self.load_seed(seedfp)


            except Exception as e:
                logging.error("Import error "+ str(e))
                pass




            for x in PmkSeed.iplugins.keys():
               klass = PmkSeed.iplugins[x]
               js = klass.getConfEntry()
               #logging.debug(js)
               context.getProcGraph().updateRegistry(json.loads(js), loc="locallocal")
               #context.getProcGraph().updateRegistry(json.loads(js), loc="locallocal")
               #context.getProcGraph().updateRegistry(json.loads(js), loc="locallocal")

            logging.debug("Registry dump: "+context.getProcGraph().dumpExternalRegistry())

            seedmonitor = PacketFileMonitor(context, context.getWorkingDir()+"seeds/", ext="py")
            seedmonitor.start()
            context.addThread(seedmonitor)

            for i in range(1,context.get_cores()+1):
                logging.info("Starting internal dispatch thread "+str(i))
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



    ###################TEST###############################3
    #try:
    #    import eggyyy
    #except ImportError:
    #    pass

    #import psutil
    #x = psutil.cpu_percent()

    #x = get_cpu_util()
    #print str(x)
    #exit(0)


    ######################################################3

    cores = multiprocessing.cpu_count()



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
    parser.add_argument('--ack',action="store_true",
                       help='turn on data packet acknowledgments.')
    parser.add_argument('--persistent',action="store_true",
                       help='keep packets on disk.')
    parser.add_argument('--brate', action='store', dest="brate", default=30,
                       help='broadcast UDP port.')
    parser.add_argument('--ghost', action='store_true',
                       help='run on a node with another pumpkin.')
    parser.add_argument('--cores', action='store', dest="cores", default=cores,
                        help='start multiple threads default for this machine is: '+str(cores))

    parser.add_argument('--group', action='store', dest="group", default="default",
                       help='node group')
    parser.add_argument('--profile',action="store_true",
                       help='profile code.')

    parser.add_argument('--rabbitmq_fallback', action='store_true',
                       help='use rabbitmq')
    parser.add_argument('--rabbitmq_broadcast', action='store_true',
                       help='use rabbitmq for broadcast')
    parser.add_argument('--rbt_host', action='store', dest='rabbitmq_host', default=None)
    parser.add_argument('--rbt_user', action='store', dest='rabbitmq_user', default=None)
    parser.add_argument('--rbt_pass', action='store', dest='rabbitmq_pass', default=None)
    parser.add_argument('--rbt_vhost', action='store', dest='rabbitmq_vhost', default=None)

    parser.add_argument('--gonzales', action='store_true',
                       help='disable certain slow features for faster streaming.')
    parser.add_argument('--buffer_size', action='store', dest="bsize", default=200,
                       help='queue size for rx/tx buffers in number of messages')

    parser.add_argument('--nocompress', action='store_true',
                       help='do not compress messages.')





    parser.add_argument('--version', action='version', version='%(prog)s '+pmk.VERSION)
    args = parser.parse_args()

    if args.shell:
        initialize_logger("./", False)
    else:
        initialize_logger("./", True)


    requests_log = logging.getLogger("tftpy")
    requests_log.setLevel(logging.WARNING)
    #logging.setLevel(logging.INFO)

    requests_log = logging.getLogger("pika")
    requests_log.setLevel(logging.WARNING)

    stun_log = logging.getLogger("pystun")
    stun_log.setLevel(logging.WARNING)



    P = Pumpkin()

    if args.daemon == "start":
        logging.info("Starting Pumpkin daemon")
        context = P.getContext()
        context.set_attributes(args)
        context.start_rxtx_buffer()
        P.start()
        root = logging.getLogger()

    if args.daemon == "stop":
        P.stop()
    if args.daemon == "restart":
        P.restart()
    if args.daemon == None:
        context = P.getContext()
        #config = ConfigParser.RawConfigParser(allow_no_value=True)
        config = ConfigParser.RawConfigParser()
        if os.path.exists(args.config):
            config.read(args.config)
            PmkShared.SUPERNODES = config.get("supernodes", "hosts").split(",")
            if args.group == "default" and config.has_option("pumpkin", "group"):
                args.group = config.get("pumpkin", "group")
            if args.rabbitmq_host == None and config.has_option("rabbitmq","host"):
                args.rabbitmq_host = config.get("rabbitmq", "host")
                args.rabbitmq_user = config.get("rabbitmq", "user")
                args.rabbitmq_pass = config.get("rabbitmq", "pass")
                if config.has_option("rabbitmq", "vhost"):
                    args.rabbitmq_vhost = config.get("rabbitmq", "vhost")

                args.rabbitmq_fallback = config.getboolean("rabbitmq", "fallback")
                args.rabbitmq_broadcast = config.getboolean("rabbitmq", "broadcast")

            if config.has_option("pumpkin","persistent"):
                args.persistent = config.get("pumpkin", "persistent")

            pass
        context.set_attributes(args)
        context.start_rxtx_buffer()
        UDP_BROADCAST_PORT = int(context.getAttributeValue().bcport)

        if not args.profile:
            P.startContext()
        else:
            profiler = cProfile.Profile()
            profiler.runctx("P.startContext()", globals(), locals())
        #Handle SIGINT
        def signal_handler(signal, frame):
                if args.profile:
                    stats = pstats.Stats(profiler)
                    stats.strip_dirs().sort_stats('cumulative').print_stats()
                    profiler.enable()

                P.stopContext()


                logging.info("Exiting Bye Bye")
                ##Ugly kill because threads zmq are not behaving
                os.system("kill -9 "+str(os.getpid()))

                sys.exit(0)


        #Catch Ctrl+C
        signal.signal(signal.SIGINT, signal_handler)
        signal.pause()
