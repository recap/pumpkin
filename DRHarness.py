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

from socket import *

from DRShared import *
from DRContexts import *
from DRInfoclient import *

VERSION = "0.1.9"

supernodes = ["flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvlight.net"]
threads = []


parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
parser.add_argument('--noplugin', action='store', dest="noplugin", default="false",
                   help='disable plugin hosting for this node.')

parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
args = parser.parse_args()

args.uid = str(gethostname())+"-"+str(uuid.uuid4())[:8]

context = MainContext(args.uid)

log.info("Node assigned UID: "+context.getUuid())

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




