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
import pika



class tracer_collector(PmkSeed.Seed):
    def __init__(self, context, poi=None):
        PmkSeed.Seed.__init__(self, context, poi)
        self.context = context
        self.exchange = context.get_group()+":stats"
        self.channel = None

        pass

    def _connect(self):
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials, virtual_host=vhost))
        self.channel = self.connection.channel()

    def on_load(self):

        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host,  credentials=credentials, virtual_host=vhost))
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=self.exchange, type='fanout')


    def run(self, pkt, data):
        #print data[0]
        if self.connection.is_closed:
                    self._connect()
        self.channel.basic_publish(exchange=self.exchange,routing_key='',body=data[0])
        #stat = self.context.get_stat()
        #self.fork_dispatch(pkt, stat, "TRACE_OUT")
        pass

