__author__ = 'reggie'

import pika
import time
import ujson as json

from Queue import *
from PmkShared import *

class mx(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)
        pass

class lx(Queue):
    def __init__(self, maxsize=0):
        Queue.__init__(self, maxsize)
        pass

class Monitor(object):

    def get_id(self):
        pass
    def connect(self, connect_to):
        pass
    def write(self, message):
        pass
    def close(self):
        pass

class LogDisptacher(SThread):
    def __init__(self, context):
        SThread.__init__(self)
        self.context = context
        self.mx = self.context.get_mx()
        self.monitors = {}

    def add_monitor(self, monitor):
        self.monitors[monitor.get_id()] = monitor

    def run(self):
        while 1:
            message = self.mx.get(True)
            for monitor in self.monitors.keys():
                mon = self.monitors[monitor]
                mon.write(message)

        pass




class RabbitMqLog(Monitor):
    def __init__(self, context):
        Monitor.__init__(self)
        self.context = context
        self.connection = None
        self.channel = None
        self.queue = None
        logging.info("Created RabbitMQMonitor")
        pass

    def __open_rabbitmq_connection(self):
        host, port, username, password, vhost = self.context.get_rabbitmq_cred()
        credentials = pika.PlainCredentials(username, password)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host, credentials=credentials, virtual_host=vhost))
        #channel = self.connection.channel()
        return connection

    def connect(self, queue):
        self.queue = queue
        self.connection = self.__open_rabbitmq_connection()
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=str(self.queue), type='fanout')
        #self.channel.queue_declare(exchange=str(self.queue), queue=str(self.queue), durable=False)

    def write(self, data):
        send = False
        message = json.dumps(data)
        while not send:
            try:
                if not self.connection.is_closed:
                    logging.info("Sending message to rabbitmq")
                    self.channel.basic_publish(exchange=str(self.queue),routing_key='',body=message)
                    send = True
                else:
                    self.connect(None)
                    logging.info("Sending message to rabbitmq")
                    self.channel.basic_publish(exchange=str(self.queue),routing_key='',body=message)
                    send = True
            except:
                time.sleep(1)
        pass