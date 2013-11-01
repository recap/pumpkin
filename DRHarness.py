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



supernodes = ["flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvalight.net"]


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
context.setSupernodeList(supernodes)

log.info("Node assigned UID: "+context.getUuid())


if not context.isWithNoPlugins():
    #Loading local modules
    onlyfiles = [ f for f in listdir("./plugins") if isfile(join("./plugins",f)) ]

    for fl in onlyfiles:
        fullpath = "./plugins/"+fl
        modname = fl[:-3]
        #ext = fl[-2:]

        if( fl[-2:] == "py"):
            log.debug("Found: "+fullpath)
            try:
                imp.load_source(modname,fullpath)
            except Exception:
                log.error("Loading Error "+ str(Exception))

    for x in DRPlugin.hplugins.keys():
       klass = DRPlugin.hplugins[x](context)
       klass.on_load()
       func = Function(klass.getpoi(), x, ("int", "int"), "int")
       context.getMePeer().addFunction(func)



peer = context.getMePeer()
#log.debug(peer.getJSON())
#pi = Peer("rasppi")
#pi.addComm(Communication("TFTP","192.168.1.51", TFTP_FILE_SERVER_PORT))
#pi.addFunction((Function("fuid", "square", "int", "int")))
#peer.addPeer(pi)


httpserv = HttpServer(context)
httpserv.start()
context.addThread(httpserv)

fm = PacketFileMonitor(context)
fm.start()
context.addThread(fm)



pktd = InternalDispatch(context)
pktd.start()
context.addThread(pktd)

pkte = ExternalDispatch(context)
pkte.start()
context.addThread(pkte)

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
            th.join()
        log.info("Exiting DataRiver")
        sys.exit(0)


#Catch Ctrl+C
signal.signal(signal.SIGINT, signal_handler)
signal.pause()




