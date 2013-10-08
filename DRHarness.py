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



supernodes = ["flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvalight.net"]


parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
parser.add_argument('--noplugin', action='store', dest="noplugin", default=False,
                   help='disable plugin hosting for this node.')
parser.add_argument('--nobroadcast', action='store', dest="nobroadcast", default=False,
                   help='disable broadcasting.')
#parser.add_argument('--supernode', action='store', dest="supernode", default=False,
#                   help='run in supernode i.e. main role is information proxy.')
parser.add_argument('--supernode',action="store_true",
                   help='run in supernode i.e. main role is information proxy.')

parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
args = parser.parse_args()


#Get a UID for this harness
args.uid = str(gethostname())+"-"+str(uuid.uuid4())[:8]

#Create a context
context = MainContext(args.uid, Peer(args.uid))
context.setArgs(args)
context.setSupernodeList(supernodes)


log.info("Node assigned UID: "+context.getUuid())


if not context.isSupernode() :
    log.debug("Running as Peer")
    tftpserver = FileServer(context, TFTP_FILE_SERVER_PORT)
    tftpserver.start()
    context.addThread(tftpserver)
    context.getMePeer().addComm(Communication("TFTP","0.0.0.0", TFTP_FILE_SERVER_PORT))

    broadcast = Broadcaster(context, UDP_BROADCAST_PORT, UDP_BROADCAST_RATE)
    broadcast.start()
    context.addThread(broadcast)

else:
    log.info("Running as SuperNode")

udplisten = BroadcastListener(context, UDP_BROADCAST_PORT)
udplisten.start()
context.addThread(udplisten)














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




