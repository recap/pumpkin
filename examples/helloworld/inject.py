__author__ = 'reggie'


###START-CONF
##{
##"object_name": "inject",
##"object_poi": "my-inject-1234",
##"auto-load": true,
##"parameters": [ ],
##"return": [
##              {
##                      "name": "name",
##                      "description": "my name",
##                      "required": true,
##                      "type": "String",
##                      "state" : "UNGREETED"
##                  }
##
##          ] }
##END-CONF




from pumpkin import *

class inject(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass


    def run(self, pkt):
        """ Data should be sourced here an injected into the
        data transformation network. In this example we are
        inject a single data "world" to a greeter which will
        tell us "hello"
        """
        self.dispatch(pkt, "World", "UNGREETED")
        #self.fork_dispatch(pkt, "Mars", "UNGREETED")
        #self.fork_dispatch(pkt, "Venus", "UNGREETED")
        #self.fork_dispatch(pkt, "Jupiter", "UNGREETED")
        #self.fork_dispatch(pkt, "Mercury", "UNGREETED")


        pass

