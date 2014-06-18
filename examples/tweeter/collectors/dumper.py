__author__ = 'reggie'


###START-CONF
##{
##"object_name": "dumper",
##"object_poi": "qpwo-2345",
##"parameters": [
##                 {
##                      "name": "tweet",
##                      "description": "",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "ENGLISH|NONENGLISH"
##                  }
##              ],
##"return": [
##
##          ] }
##END-CONF




from pumpkin import PmkSeed




class dumper(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        pass


    def run(self, pkt, tweet):
        pass
