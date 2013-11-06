__author__ = 'reggie'

import time
import os.path
import sys
import imp
import subprocess
import socket
import uuid
import argparse
import threading
import signal
import json
import zmq
import networkx as nx
#from networkx.readwrite import json_graph




import DRPlugin
import pyinotify

from os import listdir
from os.path import isfile, join
from socket import *
from pyinotify import WatchManager, Notifier, ThreadedNotifier, EventsCodes, ProcessEvent


from DRShared import *
from DRContexts import *
from DRComms import *
from DRPeers import *
from DRPackets import *
from DRDispatch import *
from DRHTTPServer import *


VERSION = "0.1.9"



parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
parser.add_argument('--noplugins',action="store_true",
                   help='disable plugin hosting for this node.')
parser.add_argument('--nobroadcast', action='store', dest="nobroadcast", default=False,
                   help='disable broadcasting.')
parser.add_argument('--supernode',action="store_true",
                   help='run in supernode i.e. main role is information proxy.')

parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
args = parser.parse_args()

######TMP TEST#############

#G = nx.DiGraph()

#x = {}
#x["state"] = "Tes"

#G.add_edge("a","b")
#G.add_edge("b","c")
#print json.dumps(G.nodes(data=True))
#strg = json_graph.dumps(G)

#GD = json_graph.loads(strg)

#for x in GD.neighbors("b"):
#    print x

#print json_graph.dumps(GD)

#sys.exit(0)

##graph = pydot.Dot(graph_type='graph')

# the idea here is not to cover how to represent the hierarchical data
# but rather how to graph it, so I'm not going to work on some fancy
# recursive function to traverse a multidimensional array...
# I'm going to hardcode stuff... sorry if that offends you

# let's add the relationship between the king and vassals
##for i in range(3):
    # we can get right into action by "drawing" edges between the nodes in our graph
    # we do not need to CREATE nodes, but if you want to give them some custom style
    # then I would recomend you to do so... let's cover that later
    # the pydot.Edge() constructor receives two parameters, a source node and a destination
    # node, they are just strings like you can see
##    edge = pydot.Edge("king", "lord%d" % i)
    # and we obviosuly need to add the edge to our graph
##    graph.add_edge(edge)

# now let us add some vassals
##vassal_num = 0
##for i in range(3):
    # we create new edges, now between our previous lords and the new vassals
    # let us create two vassals for each lord
##    for j in range(2):
##        edge = pydot.Edge("lord%d" % i, "vassal%d" % vassal_num)
##        graph.add_edge(edge)
##        vassal_num += 1

# ok, we are set, let's save our graph into a file
#graph.write_png('example1_graph.png')
##print graph.to_string()

##sys.exit(0)


#header = "POST /add/run?a=1,b=2 HTTP/1.1\r\nHost: www.google.com\r\nConnection: keep-alive\r\nAccept: application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5\r\nUser-Agent: Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_6; en-US) AppleWebKit/534.13 (KHTML, like Gecko) Chrome/9.0.597.45 Safari/534.13\r\nAccept-Encoding: gzip,deflate,sdch\r\nAvail-Dictionary: GeNLY2f-\r\nAccept-Language: en-US,en;q=0.8\r\n"
#hd = re.findall(r"(GET|POST) (?P<value>.*?)\s", header)
#pla = hd[0][1].split("?")[1]
#plb = pla[0].split("/")
#plc = pla[1].split(",")
##hd = re.findall(r"(?P<name>.*?): (?P<value>.*?)\r\n", header)
#print pla


###########################
#Get a UID for this harness
args.uid = str(gethostname())+"-"+str(uuid.uuid4())[:8]

#Create a context
context = MainContext(args.uid, Peer(args.uid))
context.setArgs(args)
context.setSupernodeList(SUPERNODES)
context.setLocalIP(get_lan_ip())

zmq_context = zmq.Context()


log.info("Node assigned UID: "+context.getUuid())
log.info("Node bound to IP: "+context.getLocalIP())

######################TMP TEST 2########################


########################################################


if context.isSupernode():
    log.debug("In supernode mode")
    udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
    udplisten.start()
    context.addThread(udplisten)

    zmqbc = ZMQBroadcaster(context, zmq_context, ZMQ_PUB_PORT)
    zmqbc.start()
    context.addThread(zmqbc)

if not context.isWithNoPlugins() and not context.isSupernode():

    #zmqsub = ZMQBroadcastSubscriber(context, zmq_context, "tcp://flightcees.lab.uvalight.net:"+str(ZMQ_PUB_PORT))
    zmqsub = ZMQBroadcastSubscriber(context, zmq_context, "tcp://127.0.0.1:"+str(ZMQ_PUB_PORT))
    zmqsub.start()
    context.addThread(zmqsub)

    onlyfiles = [ f for f in listdir("./injectors") if isfile(join("./injectors",f)) ]

    for fl in onlyfiles:
        fullpath = "./injectors/"+fl
        modname = fl[:-3]
        #ext = fl[-2:]

        if( fl[-2:] == "py"):
            log.debug("Found injector: "+fullpath)
            file_header = ""
            try:
                imp.load_source(modname,fullpath)
                fh = open(fullpath, "r")
                fhd = fh.read()
                m = re.search('##START-CONF(.+?)##END-CONF(.*)', fhd, re.S)
                if m:
                    file_header = m.group(1).replace("##","")


            except Exception:
                log.error("Loading Injector Error "+ str(Exception))
            if file_header:
                #print "HEADER: " +file_header
                d = json.loads(file_header)
                #print "DUMP: " + json.dumps(d)

    for x in DRPlugin.hplugins.keys():
       klass = DRPlugin.hplugins[x](context)
       klass.on_load()
       klass()

    #Loading local modules
    onlyfiles = [ f for f in listdir("./plugins") if isfile(join("./plugins",f)) ]

    for fl in onlyfiles:
        fullpath = "./plugins/"+fl
        modname = fl[:-3]
        #ext = fl[-2:]

        if( fl[-2:] == "py"):
            log.debug("Found module: "+fullpath)
            file_header = ""
            try:
                imp.load_source(modname,fullpath)
                fh = open(fullpath, "r")
                fhd = fh.read()
                m = re.search('##START-CONF(.+?)##END-CONF(.*)', fhd, re.S)
                if m:
                    conf = m.group(1).replace("##","")
                    if conf:
                        d = json.loads(conf)
                        klass = DRPlugin.hplugins[modname](context)
                        DRPlugin.iplugins[modname] = klass
                        klass.on_load()
                        klass.setconf(d)
                        #print klass.getparameters()
                        #print klass.getreturn()


            except Exception:
                log.error("Loading Error "+ str(Exception))
            #if file_header:
                #print "HEADER: " +file_header
             #   d = json.loads(file_header)
                #print "DUMP: " + json.dumps(d)

    #msg = "["
    for x in DRPlugin.iplugins.keys():
       klass = DRPlugin.iplugins[x]

       js = '{ "name" : "'+klass.getname()+'", \
       "zmq_endpoint" : [ {"ep" : "'+context.endpoints[0]+'", "cuid" : "'+context.getUuid()+'"} ],' \
       ''+klass.getparameters()+',' \
       ''+klass.getreturn()+'}'
       context.updateRegistry(json.loads(js))
    #   msg = msg + js +","


    dstr = context.dumpRegistry()
    #djson = json.loads(dstr)

    #print djson["add"]

    #for t in djson.keys():
    #    print djson[t]["itype"]
    #msg = msg[0:len(msg)-1] + "]"
    #log.debug(msg)
    #hj = json.loads(msg)
    #print hj[0]["zmq_endpoint"][0]
    announce(dstr)


       #func = Function(klass.getpoi(), x, ("int", "int"), "int")
       #context.getMePeer().addFunction(func)


#peer = context.getMePeer()
#log.debug(peer.getJSON())
#pi = Peer("rasppi")
#pi.addComm(Communication("TFTP","192.168.1.51", TFTP_FILE_SERVER_PORT))
#pi.addFunction((Function("fuid", "square", "int", "int")))
#peer.addPeer(pi)


#httpserv = HttpServer(context)
#httpserv.start()
#context.addThread(httpserv)

#fm = PacketFileMonitor(context)
#fm.start()
#context.addThread(fm)

#    zmq_context = zmq.Context()

#    tcpm = ZMQPacketMonitor(context, zmq_context, "inproc://backbus")
#    tcpm.start()
#    context.addThread(tcpm)

#    time.sleep(2)

#    dsp = ZMQPacketDispatch(context, zmq_context, "inproc://backbus")
#    dsp.start()
#    context.addThread(dsp)



#context.getTx().put(pkt)

#    pktd = InternalDispatch(context)
#    pktd.start()
#    context.addThread(pktd)

#fh = open("./d1.pkt", "r")
#pkt = fh.read()

#context.getRx().put(pkt)

#pkte = ExternalDispatch(context)
#pkte.start()
#context.addThread(pkte)

#if not context.isSupernode() :
#    log.debug("Running as Peer")
#    tftpserver = FileServer(context, TFTP_FILE_SERVER_PORT)
#    tftpserver.start()
#    context.addThread(tftpserver)
#    context.getMePeer().addComm(Communication("TFTP","0.0.0.0", TFTP_FILE_SERVER_PORT))
#
#    broadcast = Broadcaster(context, UDP_BROADCAST_PORT, UDP_BROADCAST_RATE)
#    broadcast.start()
#    context.addThread(broadcast)
#
#else:
#    log.info("Running as SuperNode")
#    #rzvs = RendezvousServer(context, RZV_SERVER_PORT)
#    #rzvs.start()
#    #context.addThread(rzvs)
#
#udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
#udplisten.start()
#context.addThread(udplisten)



#Handle SIGINT
def signal_handler(signal, frame):
        for th in context.getThreads():
            th.stop()
            #th.join()

        time.sleep(5)
        log.info("Exiting DataRiver")
        #os.system("./forcekill")
        sys.exit(0)


#Catch Ctrl+C
signal.signal(signal.SIGINT, signal_handler)
signal.pause()




