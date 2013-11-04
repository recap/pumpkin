__author__ = 'reggie'

###START-CONF
##{
##"object_name": "add",
##"object_poi": "qpwo-2345-qw-21",
##"parameters": [
##                  {
##                      "name": "number_list",
##                      "description": "csv of numbers",
##                      "required": true,
##                      "type": "CSVStringNumbers",
##                      "format": "csv",
##                      "istate" : "ANY"
##                  }
##              ],
##"return": [
##            {
##              "name": "summed_numbers",
##              "description": "summed number list",
##              "type" : "StringNumber",
##              "ostate": "BIG|SMALL"
##            }
##          ] }
##END-CONF

import DRPlugin

from time import sleep


class add(DRPlugin.PluginBase):
    def __init__(self, context, poi=None):
        DRPlugin.PluginBase.__init__(self, context,poi)
        #self.istate["sequence_size"] = "ANY"
        #self.ostate["number_size"] = "SMALL|BIG"
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        #self.poi = "qaz-123"
        pass

    #def __call__(self, *args, **kwargs):
    #    x = 10
    #    return x

    #def sub(self, *args):
    #    print "Running: " + self.__class__.__name__
    #    sp = args[0].split(",")


    #    count = 0
    #    for s in sp:
    #        count = count - int(s)

    #    return count
    #    pass

    def __call__(self, *args, **kwargs):
        print "Running: " + self.__class__.__name__
        sp = args[0].split(",")


        count = 0
        for s in sp:
            count = count + int(s)

        return count
        pass

    def run(self, *args):
        THRESHOLD = 999
        print "Running: " + self.__class__.__name__
        sp = args[0].split(",")


        count = 0
        for s in sp:
            count = count + int(s)

        if count > THRESHOLD:
            self.ostate["number_size"] = "BIG"
        else:
            self.ostate["number_size"] = "SMALL"

        return count
        pass

    #def sayhello(self):
    #    return "hello"


    def on_unload(self):
        print "Unloading: " + self.__class__.__name__
        pass
