__author__ = 'reggie'

import time
import os.path
import sys
import subprocess
import socket
import uuid
import argparse
import threading
import signal
import json

from socket import *


from DRShared import *
from DRContexts import *
from DRComms import *
from DRPeers import *

VERSION = "0.1.9"


##data = json.loads('{"maps":[{"id":"blabla","iscategorical":"0"},{"id":"2blabla","iscategorical":"0"}],\
##"masks":{"id":"valore"},\
##"om_points":"value",\
##"parameters":{"id":"valore"}\
##}')
#
##data = json.loads('{"uid" : "my-uid", "udp_port" : "7780"}')
##data = json.loads('{uid : "my-uid", udp_port : "7780"}')
#
##print data["uid"]
##print data["udp_port"]
##print data["maps"][1]["id"]
##print data["masks"]["id"]
##print data["om_points"]
#
##func = Function("10.100.qaz", "add", "someinput", "someoutput")
##print "[1] :" + func.getJSON()
##fun2 = Function.fromJSON(func.getJSON())
#
##print "[2] :"+ fun2.getJSON()
#
#comm = Communication("TFTP", "127.0.0.1", "7780")
#comm2 = Communication("P2PTFTP", "192.168.1.1", "7770")
#func = Function("10.100.qaz", "add", "someinput", "someoutput")
#func2 = Function("10.111.plo", "sub", "someinput", "someoutput")
#peer = Peer("slimem-1qsad")
#peer.addComm(comm)
##peer.addComm(comm2)
##peer.addFunction(func)
##peer.addFunction(func2)
#
#peer2 =  Peer("nano-plko123")
##peer2.addComm(comm)
#peer2.addComm(comm2)
##peer2.addFunction(func)
##peer2.addFunction(func2)
#
#peer3 = Peer("pico-qaw34")
#peer3.addComm(comm)
##peer3.addComm(comm2)
##peer3.addFunction(func)
##peer3.addFunction(func2)
#
#peer2.addPeer(peer3)
#peer.addPeer(peer2)
#for pc in peer2.comms:
#    print "KOLOK: " + pc.type
#
#
#print peer.getJSON()
#d = json.loads(peer.getJSON())
#
#for p in d["peers"]:
#    print p["uid"]
#    for x in p["comms"]:
#        print x["type"]
#
##for f in d["functions"]:
##    print f["poi"]
#
##print d["comms"][1]["port"]


supernodes = ["flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvlight.net"]
threads = []


parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
parser.add_argument('--noplugin', action='store', dest="noplugin", default="false",
                   help='disable plugin hosting for this node.')

parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
args = parser.parse_args()

args.uid = str(gethostname())+"-"+str(uuid.uuid4())[:8]
peer = Peer(args.uid)
context = MainContext(args.uid, peer)



log.info("Node assigned UID: "+context.getUuid())

tftpserver = FileServer(context, TFTP_FILE_SERVER_PORT)
tftpserver.start()
threads.append(tftpserver)
peer.addComm(Communication("TFTP","0.0.0.0", TFTP_FILE_SERVER_PORT))

udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
udplisten.start()

threads.append(udplisten)

broadcast = Broadcaster(context, UDP_BROADCAST_PORT, UDP_BROADCAST_RATE)
broadcast.start()

threads.append(broadcast)












#Handle SIGINT
def signal_handler(signal, frame):
        for th in threads:
            th.stop()
            th.join()
        log.info("Exiting DataRiver")
        sys.exit(0)


#Catch Ctrl+C
signal.signal(signal.SIGINT, signal_handler)
signal.pause()




