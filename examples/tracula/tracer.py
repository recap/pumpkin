__author__ = 'reggie'


###START-CONF
##{
##"object_name": "tracer",
##"object_poi": "tracer-0001",
##"auto-load": true,
##"parameters": [
##              {
##                      "name": "",
##                      "type": "Internal",
##                      "state" : "TRACE"
##                }
## ],
##"return": [
##              {
##                      "name": "",
##                      "type": "Internal",
##                      "state" : "TRACE_OUT"
##                }
##
##          ] }
##END-CONF



from subprocess import call

from pumpkin import *

import os, sys, stat



class tracer(PmkSeed.Seed):
    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context, poi)
        self.context = context

        pass


    def run(self, pkt, data):
        stat = self.context.get_stat()
        self.fork_dispatch(pkt, stat, "TRACE_OUT")
        pass

