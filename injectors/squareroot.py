__author__ = 'reggie'

###START-CONF
##{
##"object_name": "squareroot",
##"object_poi": "qpwo-2345-qw-28",
##"parameters": [
##                  {
##                      "name": "number",
##                      "description": "number entry",
##                      "required": true,
##                      "type": "StringNumber",
##                      "format": "string",
##                      "state" : "BIG"
##                  }
##              ],
##"return": [
##            {
##              "name": "squareroot_number",
##              "description": "squared root number",
##              "type" : "StringNumber",
##              "state": "SQUAREDROOT"
##            }
##          ] }
##END-CONF

import DRPlugin
import math

from time import sleep


class squareroot(DRPlugin.PluginBase):
    def on_load(self):
        print "Loading: " + self.__class__.__name__
        self.poi =" qaz-456"
        pass

    def run(self, *args):
        print "Running: " + self.__class__.__name__
        sqr = 0
        sqr = math.sqrt(int(args[0]))
        return sqr
        pass
    

    def on_unload(self):
        print "Unloading: " + self.__class__.__name__
        pass
