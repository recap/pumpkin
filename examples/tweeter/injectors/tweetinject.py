###START-CONF
##{
##"object_name": "tweetinject",
##"object_poi": "qpwo-2345",
##"auto-load": true,
##"remoting" : false,
##"parameters": [
##
##              ],
##"return": [
##              {
##                      "name": "tweet",
##                      "description": "raw tweet",
##                      "required": true,
##                      "type": "TweetString",
##                      "format": "",
##                      "state" : "RAW"
##                  }
##
##          ] }
##END-CONF



from os import listdir
from os.path import isfile, join
import pika
from os.path import expanduser
from pumpkin import *

class tweetinject(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.connection = None
        self.channel = None

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        self.connection, self.channel = self.__open_rabbitmq_channel()

    def __open_rabbitmq_channel(self):
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,  credentials=credentials, virtual_host=vhost))
        channel = connection.channel()
        return (connection, channel)


    def run(self, pkt):
        dir = expanduser("~")+"/tweets/"
        onlyfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) ]
        for fl in onlyfiles:
            fullpath = dir+fl
            if( fl[-3:] == "txt"):
                print "File: "+str(fl)
                with open(fullpath) as f:
                    for line in f:
                        if line.startswith('T'):
                            tweet = line
                        if line.startswith("U"):
                            tweet = tweet + line
                        if line.startswith("W"):
                            if line == "No Post Title":
                                line =""
                            else:
                                tweet = tweet + line
                                self.dispatch(pkt, tweet, "RAW")
                                del line
                                del tweet

    def publish(self, data, queue):
        self.channel.basic_publish(exchange='',
                              routing_key=queue,
                              body=str(data))

