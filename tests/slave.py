__author__ = 'reggie'

###START-CONF
##{
##"object_name": "slave",
##"object_poi": "my-slave-1234",
##"auto-load" : true,
##"parameters": [ {
##                  "name": "name",
##                  "description": "name to slave",
##                  "required": true,
##                  "type": "X",
##                  "state" : "SLAVE"
##              } ],
##"return": [
##              {
##                  "name": "greeting",
##                  "description": "a greeting",
##                  "required": true,
##                  "type": "X",
##                  "state" : "MASTER"
##               }
##
##          ] }
##END-CONF




from pumpkin import *

class slave(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):
        self.dict_count = {}
        pass


    def run(self, pkt, data):
        tokens = str(data[0]).split()
        for t in tokens:
            self.update_dict(t, self.dict_count)

        self.dispatch(pkt, json.dumps(self.dict_count), "MASTER")
        self.dict_count = {}
        pass

    def update_dict(self, token, dict):
        if token in dict.keys():
            dict[token] += 1
        else:
            dict[token] = 1

    def dump_dict(self, dict):
        for k in dict.keys():
            print k+" "+str(dict[k])

