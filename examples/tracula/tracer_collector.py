__author__ = 'reggie'


###START-CONF
##{
##"object_name": "tracer_collector",
##"object_poi": "tracer_collector-0001",
##"auto-load": true,
##"parameters": [
##              {
##                      "name": "",
##                      "type": "Internal",
##                      "state" : "TRACE_OUT"
##                }
## ],
##"return": [
##              {
##                      "name": "",
##                      "type": "Internal",
##                      "state" : "TRACE"
##                }
##
##          ] }
##END-CONF



from subprocess import call

from pumpkin import *

import os, sys, stat



class tracer_collector(PmkSeed.Seed):
    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context, poi)
        self.context = context

        pass


    def run(self, pkt, data):
        print data[0]
        #stat = self.context.get_stat()
        #self.fork_dispatch(pkt, stat, "TRACE_OUT")
        pass

