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
import urllib2

from os.path import expanduser
from pumpkin import *

class tweetinject(PmkSeed.Seed):

    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context,poi)
        self.connection = None
        self.channel = None

    def get_net_file(self, url, file_name):
        #file_name = url.split('/')[-1]
        downloaded = False
        while not downloaded:
            try:
                u = urllib2.urlopen(url)
                f = open(file_name, 'wb')
                meta = u.info()
                file_size = int(meta.getheaders("Content-Length")[0])
                self.logger.info ("Downloading: %s Bytes: %s" % (file_name, file_size))

                file_size_dl = 0
                block_sz = 8192
                while True:
                    buffer = u.read(block_sz)
                    if not buffer:
                        break

                    file_size_dl += len(buffer)
                    f.write(buffer)
                    #status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
                    #status = status + chr(8)*(len(status)+1)
                    #print status,
                f.close()
                downloaded = True
            except Exception as e:
                self.logger.error("Error downloading, trying again....")
                time.sleep(5)
                pass

    def on_load(self):
        print "Loading: " + self.__class__.__name__
        ok = False
        url = "http://elab.lab.uvalight.net/gzs/tweets2009-06-brg.txt"
        dir = expanduser("~")+"/tweets/"
        output_file = dir+"tweets2009-06-brg.txt"
        self._ensure_dir(dir)
        onlyfiles = [ f for f in listdir(dir) if isfile(join(dir,f)) ]

        for fl in onlyfiles:
            fullpath = dir+fl
            if( fl[-3:] == "txt"):
                ok = True
                break

        if not ok:
            self.get_net_file(url,output_file)


        #self.connection, self.channel = self.__open_rabbitmq_channel()

    # def __open_rabbitmq_channel(self):
    #     host, port, username, password, vhost = self.context.get_rabbitmq_cred()
    #     credentials = pika.PlainCredentials(username, password)
    #     connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,  credentials=credentials, virtual_host=vhost))
    #     channel = connection.channel()
    #     return (connection, channel)


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
                                self.fork_dispatch(pkt, tweet, "RAW")
                                del line
                                del tweet

    # def publish(self, data, queue):
    #     self.channel.basic_publish(exchange='',
    #                           routing_key=queue,
    #                           body=str(data))

