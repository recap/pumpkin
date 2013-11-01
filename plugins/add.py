__author__ = 'reggie'

##START-DESC##
#{
#    "apiVersion": "1.0.0",
#    "moduleObjectId: "qas-89312"
#    "module": "mathfuncs",
#    "operations": [
#        {
#           "name" : "add"
#           "summary" : "Adding numbers"
#           "notes" : "No notes"
#            "path": "/mathfuncs/add",
#            "operations": [
#               "method": "GET",
#                    "summary": "Add numbers",
#                    "notes": "Returns a number",
#                    "type": "string",
#                    "nickname": "add",
#                    "parameters": [
#                        {
#                            "name": "petId",
#                            "description": "ID of pet that needs to be fetched",
#                            "required": true,
#                            "type": "integer",
#                            "format": "int64",
#                            "paramType": "path",
#                            "minimum": "1.0",
#                            "maximum": "100000.0"
#                        }
#                    ],
#
##END-DESC##

import DRPlugin

from time import sleep


class add(DRPlugin.PluginBase):
    def __init__(self, context, poi=None):
        DRPlugin.PluginBase.__init__(self, context,poi)
        print "HELLO"
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        self.poi = "qaz-123"
        pass

    def __call__(self, *args, **kwargs):
        x = 10
        return x

    def sub(self, *args):
        print "Running: " + self.__class__.__name__
        sp = args[0].split(",")


        count = 0
        for s in sp:
            count = count - int(s)

        return count
        pass
    def run(self, *args):
        print "Running: " + self.__class__.__name__
        sp = args[0].split(",")


        count = 0
        for s in sp:
            count = count + int(s)

        return count
        pass

    def sayhello(self):
        return "hello"


    def on_unload(self):
        print "Unloading: " + self.__class__.__name__
        pass
