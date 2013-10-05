__author__ = 'reggie'

import time
import os.path
import sys
import subprocess
import socket
import uuid
import argparse
import threading

from socket import *

from DRShared import *
from DRContexts import *
from DRInfoclient import *

VERSION = "0.1.9"

supernodes = ["flightcees.lab.uvalight.net", "mike.lab.uvalight.net", "elab.lab.uvlight.net"]


parser = argparse.ArgumentParser(description='Harness for Datafluo jobs')
parser.add_argument('--noplugin', action='store', dest="noplugin", default="false",
                   help='disable plugin hosting for this node.')

parser.add_argument('--version', action='version', version='%(prog)s '+VERSION)
args = parser.parse_args()

args.uid = str(gethostname())+"-"+str(uuid.uuid4())[:8]

context = MainContext(args.uid)

log.info("Node assigned UID: "+context.getUuid())


udplisten = BroadcastListener(UDP_BROADCAST_PORT)
udplisten.start()

log.info("Establishing network connectivity...")


s = socket(AF_INET, SOCK_DGRAM)
s.bind(('', 0))
s.setsockopt(SOL_SOCKET, SO_BROADCAST, 1)

data = repr(time.time()) + '\n'
data = context.getUuid() + '\n'
s.sendto(data, ('<broadcast>', UDP_BROADCAST_PORT))




#sr = socket(AF_INET, SOCK_DGRAM)
#sr.bind(('', UDP_BROADCAST_PORT))

#while 1:
#    data, wherefrom = sr.recvfrom(1500, 0)
#    sys.stderr.write(repr(wherefrom) + '\n')
#    sys.stdout.write(data)



#time.sleep(2)




