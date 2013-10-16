__author__ = 'reggie'
import re
import threading
from threading import *
from socket import *
import DRPlugin

from DRShared import *


class Head(object):

    def __init__(self, header):
        self.params_hash = {}
        self.params_list = []
        self.params_string = ""
        hd = re.findall(r"(GET|POST) (?P<value>.*?)\s", header)
        parlist = hd[0][1].split("?")[1].split("&")
        serv = hd[0][1].split("?")[0].split("/")
        self.module = serv[len(serv)-2]
        self.method = serv[len(serv)-1]

        for p in parlist:
            key = p.split("=")[0]
            value = p.split("=")[1]
            self.params_hash[key] = value
            self.params_list.append(value)
            self.params_string += str(value)+","

        self.params_string = self.params_string[:-1]


class HttpServer(Thread):


    def __init__(self, context):
        Thread.__init__(self)
        self.context = context
        pass

    def run(self):

        s = socket(AF_INET, SOCK_STREAM)
        s.bind((HTTP_TCP_IP, HTTP_TCP_PORT))
        s.listen(1)
        while 1:
            conn, addr = s.accept()
            log.debug('HTTP Connection address:'+ str(addr))

            data = conn.recv(HTTP_BUFFER_SIZE)
            if not data: break
            log.debug("HTTP data: "+data)
            if "favicon" in data:
                conn.close()
                continue

            h = Head(data)
            if h.module in DRPlugin.hplugins.keys():
                klass = DRPlugin.hplugins[h.module](self.context)
                klass.on_load()
                rt = klass.run(h.params_string)
                print rt
                conn.send(str(rt))
            else:
                log.warn("Trying to invoke module with HTTP: "+h.module+" but doe not exist.")

            conn.close()
        pass



